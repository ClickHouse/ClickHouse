import json
import base64

def _finalize_order(nonce_map, hostname, request_data, order_id):
    b64_protected = request_data["protected"]
    protected = json.loads(base64.b64decode(b64_protected + "===").decode())
    print(protected)

    nonce = protected["nonce"]
    if not nonce_map[nonce]:
        print(f"Nonce not found: {nonce} in map {nonce_map}")
        return "Nonce not found", 400
    nonce_map[nonce] = False

    url = protected["url"]
    if url != f"https://{hostname}/acme/finalize/{order_id}":
        print(url, f"https://{hostname}/acme/finalize/{order_id}")
        return "Invalid URL", 400

    b64_payload = request_data["payload"]
    payload = json.loads(base64.b64decode(b64_payload + "===").decode())
    print(payload)

    csr = payload["csr"]

    response = {
        "status": "ready",
        "expires": "2024-11-11T11:11:11Z",
        "identifiers": [
            {
                "type": "dns",
                "value": "example.com",
            }
        ],
        "authorizations": [f"https://{hostname}/acme/authz/1"],
        "finalize": f"https://{hostname}/acme/finalize/1",
    }

    print("Finalize OK")

    return json.dumps(response), 200, csr
