import json

def _describe_order(order_map, hostname, order_id):
    response = {
        "status": order_map[order_id],
        "expires": "2024-11-11T11:11:11Z",
        "identifiers": [
            {
                "type": "dns",
                "value": "example.com",
            }
        ],
        "authorizations": [f"https://{hostname}/acme/authz/{order_id}"],
        "finalize": f"https://{hostname}/acme/finalize/{order_id}",
        "certificate": f"https://{hostname}/cert/{order_id}",
    }

    return json.dumps(response), 200
