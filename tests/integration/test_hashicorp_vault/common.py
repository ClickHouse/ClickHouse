import requests
from helpers.cluster import ClickHouseCluster
import logging


def vault_startup_command(cluster):

    payload = {
        "data": {
            "password": "test_password",
        }
    }

    custom_headers = {
        "X-Vault-Token": "foobar",
    }

    response = requests.post(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/secret/data/username",
        json=payload,
        headers=custom_headers,
    )
    assert response.status_code == 200
