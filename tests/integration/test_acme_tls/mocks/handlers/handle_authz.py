import json

def _process_authz(hostname, order_id):
    response = {
        "status": "pending",
        "expires": "2024-11-11T11:11:11Z",
        "identifiers": [
            {
                "type": "dns",
                "value": "example.com",
            }
        ],
        "challenges": [
            {
                "type": "http-01",
                "url": f"https://{hostname}/acme/chall/{order_id}",
                "status": "pending",
                "token": f"token{order_id}",
            },
            {
                "type": "trash-01",
                "url": f"https://{hostname}/acme/chall/{order_id}",
                "status": "pending",
                "token": "token12345",
            },
        ],
    }

    return json.dumps(response), 200
