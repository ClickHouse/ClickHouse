import requests


def vault_startup_command():

    payload = {
        "data": {
            "http": "8200",
            "max_connections": "<max_connections>5000</max_connections>",
            "password": "test_password",
        }
    }

    custom_headers = {
        "X-Vault-Token": "foobar",
    }

    requests.post(
        "http://localhost:1337/v1/secret/data/ch_secret",
        json=payload,
        headers=custom_headers,
    )
