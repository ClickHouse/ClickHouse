import json
import base64
import urllib.request
import hashlib

def _process_challenge(jwk_map, nonce_map, hostname, request_data, order_id):
    b64_protected = request_data["protected"]
    protected = json.loads(base64.b64decode(b64_protected + "===").decode())
    print(protected)

    jwk = jwk_map[protected["kid"]]

    nonce = protected["nonce"]
    if not nonce_map[nonce]:
        print(f"Nonce not found: {nonce} in map {nonce_map}")
        return "Nonce not found", 400
    nonce_map[nonce] = False

    url = protected["url"]
    if url != f"https://{hostname}/acme/chall/{order_id}":
        print(url, f"https://{hostname}/acme/chall/{order_id}")
        return "Invalid URL", 400

    response = {
        "status": "valid",
        "url": f"https://{hostname}/acme/chal/{order_id}",
        "token": f"token{order_id}",
    }

    request = urllib.request.Request(
        f"http://localhost:8123/.well-known/acme-challenge/token{order_id}"
    )
    with urllib.request.urlopen(request) as res:
        contents = str(res.read().decode())
        print(contents)

        token, hash = contents.split(".")
        if token != f"token{order_id}":
            return f"Invalid token, {token} != token{order_id}", 400

        jwt_sha256 = hashlib.sha256(jwk.encode()).digest()
        base64_jwt_sha256 = (
            base64.urlsafe_b64encode(jwt_sha256).decode().replace("=", "")
        )

        if hash != base64_jwt_sha256:
            return f"Invalid hash, {hash} != {base64_jwt_sha256}, jwk: {jwk}", 400

    print("Challenge OK")

    return json.dumps(response), 200
