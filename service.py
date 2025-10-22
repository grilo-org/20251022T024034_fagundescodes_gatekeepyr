from fastapi import FastAPI
import asyncio
import random
from datetime import datetime

app = FastAPI()


@app.get("/")
async def root():
    delay = random.uniform(0.1, 0.5)
    await asyncio.sleep(delay)
    return {
        "service": "service",
        "message": "Request processed with success",
        "timestamp": datetime.now().isoformat(),
        "delay": delay,
    }


@app.get("/health")
async def health():
    return {"status": "Ok"}
