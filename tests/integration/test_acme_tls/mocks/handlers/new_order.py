import base64
import json

def _new_order(order_map, nonce_map, hostname, request_data):
    b64_protected = request_data["protected"]
    protected = json.loads(base64.b64decode(b64_protected + "===").decode())
    print(protected)

    nonce = protected["nonce"]
    if not nonce_map[nonce]:
        print(f"Nonce not found: {nonce} in map {nonce_map}")
        return "Nonce not found", 400
    nonce_map[nonce] = False

    url = protected["url"]
    if url != f"https://{hostname}/acme/new-order":
        print(url, f"https://{hostname}/acme/new-order")
        return "Invalid URL", 400

    b64_payload = request_data["payload"]
    payload = json.loads(base64.b64decode(b64_payload + "===").decode())
    print(payload)

    order_num = len(order_map)
    order_map[order_num] = "pending"

    response = {
        "status": "pending",
        "expires": "2024-11-11T11:11:11Z",
        "identifiers": payload["identifiers"],
        "authorizations": [f"https://{hostname}/acme/authz/{order_num}"],
        "finalize": f"https://{hostname}/acme/finalize/{order_num}",
    }

    print("Order OK")

    return json.dumps(response), 201
