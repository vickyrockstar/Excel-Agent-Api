import pandas as pd
from fastapi import UploadFile, File
import os
from fastapi.responses import FileResponse
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import re
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="Mini LLM Agent for Data Cleaning")

# Allow frontend (localhost during development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["http://localhost:5173"] for stricter control
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Define input and output schemas
class InputData(BaseModel):
    company_name: str
    email_paragraph: str
    address: str

class OutputData(BaseModel):
    cleaned_name: str
    emails: list[str]
    street: str | None
    city: str | None
    state: str | None
    zip_code: str | None


# Utility functions
def clean_company_name(name: str) -> str:
    # Normalize: remove dots and commas first
    name = re.sub(r"[.,]", "", name)

    # Remove multiple known suffixes using loop or combined regex
    suffixes = ["LLC", "INC", "CORP", "LTD", "INCORPORATED", "CORPORATION", "LIMITED"]
    words = name.split()
    cleaned_words = [word for word in words if word.upper() not in suffixes]

    return " ".join(cleaned_words).strip()

def extract_emails(paragraph: str) -> list[str]:
    paragraph = paragraph.strip()
    matches = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", paragraph)
    return matches

def parse_address(address: str) -> dict:
    parts = address.split(",")
    if len(parts) < 3:
        return {"street": None, "city": None, "state": None, "zip_code": None}

    street = parts[0].strip()
    city = parts[1].strip()
    state_zip = parts[2].strip().split()

    state = state_zip[0] if len(state_zip) >= 1 else None
    zip_code = state_zip[1] if len(state_zip) >= 2 else None

    return {
        "street": street,
        "city": city,
        "state": state,
        "zip_code": zip_code
    }

# API endpoint
@app.post("/clean", response_model=OutputData)
def clean_data(data: InputData):
    cleaned_name = clean_company_name(data.company_name)
    emails = extract_emails(data.email_paragraph)
    address_parts = parse_address(data.address)

    return OutputData(
        cleaned_name=cleaned_name,
        emails=emails,
        street=address_parts["street"],
        city=address_parts["city"],
        state=address_parts["state"],
        zip_code=address_parts["zip_code"]
    )

@app.post("/upload_excel")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith(".xlsx"):
        return {"error": "Only .xlsx files are supported."}

    # Save uploaded file
    input_path = f"uploads/{file.filename}"
    with open(input_path, "wb") as f:
        f.write(await file.read())

    # Read the Excel
    df = pd.read_excel(input_path)
    cleaned_rows = []

    for _, row in df.iterrows():
        try:
            company = str(row.get("Company Name", ""))
            email_text = str(row.get("Email (Paragraph)", ""))
            address = str(row.get("Address", ""))

            cleaned_name = clean_company_name(company)
            emails = extract_emails(email_text)
            addr = parse_address(address)

            cleaned_rows.append({
                "Company Name": cleaned_name,
                "Emails": ", ".join(emails),  # Combine emails into one string
                "Street": addr["street"],
                "City": addr["city"],
                "State": addr["state"],
                "Zip Code": addr["zip_code"]
            })

        except Exception as e:
            cleaned_rows.append({
                "Company Name": company,
                "Emails": "ERROR",
                "Street": None,
                "City": None,
                "State": None,
                "Zip Code": None
            })

    # Save cleaned Excel
    cleaned_df = pd.DataFrame(cleaned_rows)
    output_path = f"cleaned/cleaned_{file.filename}"
    cleaned_df.to_excel(output_path, index=False)

    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"cleaned_{file.filename}"
    )

