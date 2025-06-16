import os
import json
import requests
import datetime
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    query_param = req.params.get('symbols')
    symbols = query_param.split(',') if query_param else ['AAPL', 'MSFT', 'GOOG', 'TSLA', 'AMZN', 'NVDA', 'META']

    now = datetime.datetime.utcnow()
    iso_time = now.isoformat() + "Z"

    api_key = os.getenv("TWELVE_API_KEY")
    conn_str = os.getenv("BLOB_CONN_STR")

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client("raw-prices")

    saved = []
    failed = []

    for symbol in symbols:
        try:
            res = requests.get(
                "https://api.twelvedata.com/price",
                params={"symbol": symbol, "apikey": api_key}
            )

            if res.ok:
                price_data = res.json()
                payload = {
                    "symbol": symbol,
                    "price": price_data.get("price"),
                    "fetched_at": iso_time
                }

                blob_path = f"{now.year}/{now.month:02}/{now.day:02}/{symbol}_{now.strftime('%Y-%m-%dT%H-%M-%S')}.json"
                container.upload_blob(name=blob_path, data=json.dumps(payload), overwrite=True)

                print(f"[OK] Stored {symbol} at {blob_path}")
                saved.append(symbol)
            else:
                print(f"[ERR] Couldn't get price for {symbol} – {res.text}")
                failed.append(symbol)

        except Exception as e:
            print(f"[EXC] Problem with {symbol}: {e}")
            failed.append(symbol)

    summary = f"Saved: {saved}\nFailed: {failed}" if failed else f"Saved all: {saved}"
    return func.HttpResponse(summary, status_code=207 if failed else 200)