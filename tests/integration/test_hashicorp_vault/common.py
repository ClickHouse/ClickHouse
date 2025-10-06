import requests


def vault_startup_command():

    payload = {
        "data": {
            "password": "test_password",
        }
    }

    custom_headers = {
        "X-Vault-Token": "foobar",
    }

    requests.post(
        "http://localhost:8200/v1/secret/data/username",
        json=payload,
        headers=custom_headers,
    )
