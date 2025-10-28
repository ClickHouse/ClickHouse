import requests
from helpers.cluster import ClickHouseCluster


def vault_startup_command(cluster):

    # enable user/password auth
    payload = {"type": "userpass"}

    custom_headers = {
        "X-Vault-Token": "foobar",
    }

    response = requests.post(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/sys/auth/userpass",
        json=payload,
        headers=custom_headers,
    )
    assert response.status_code == 204

    # update admins policy to manage secrets
    payload = {"policy": 'path "*" {capabilities = ["create", "read"]}'}

    custom_headers = {
        "X-Vault-Token": "foobar",
    }

    response = requests.post(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/sys/policies/acl/admins",
        json=payload,
        headers=custom_headers,
    )
    assert response.status_code == 204

    # create user
    payload = {"password": "test", "token_policies": ["admins"]}

    custom_headers = {
        "X-Vault-Token": "foobar",
    }

    response = requests.post(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/auth/userpass/users/user1",
        json=payload,
        headers=custom_headers,
    )
    assert response.status_code == 204

    # write secret
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
