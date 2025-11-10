import json
import base64
import hashlib

def _new_account(nonce_map, jwk_map, hostname, request_data):
    b64_protected = request_data["protected"]
    protected = json.loads(base64.b64decode(b64_protected + "===").decode())
    print(protected)

    nonce = protected["nonce"]
    if not nonce_map[nonce]:
        print(f"Nonce not found: {nonce} in map {nonce_map}")
        return "Nonce not found", 400
    nonce_map[nonce] = False

    url = protected["url"]
    if url != f"https://{hostname}/acme/new-acct":
        print(url, f"https://{hostname}/acme/new-acct")
        return "Invalid URL", 400

    b64_payload = request_data["payload"]
    payload = json.loads(base64.b64decode(b64_payload + "===").decode())
    print(payload)

    if payload["termsOfServiceAgreed"] is not True:
        return "Terms of service not agreed", 400
    if len(payload["contact"]) != 1:
        return "Invalid contact", 400

    json_str = json.dumps(
        protected["jwk"], separators=(",", ":"), ensure_ascii=False
    )
    kid = hashlib.sha256(json_str.encode()).hexdigest()
    jwk_map[kid] = json_str

    response = {
        "key": protected["jwk"],
        "contact": payload["contact"],
        "createdAt": "2024-11-11T11:11:11Z",
        "status": "valid",
    }

    print("Auth OK")

    return json.dumps(response), 201, kid
