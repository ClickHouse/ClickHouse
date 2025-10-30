import requests
from helpers.cluster import ClickHouseCluster


def send_post_request(cluster, path, payload, status_code=200):
    custom_headers = {
        "X-Vault-Token": "foobar",
    }
    response = requests.post(
        f"http://{cluster.hashicorp_vault_ip}:8200/v1/{path}",
        json=payload,
        headers=custom_headers,
    )
    assert response.status_code == status_code


def vault_startup_command(cluster):
    # write secret
    payload = {
        "data": {
            "password": "test_password",
        }
    }
    send_post_request(cluster, "secret/data/username", payload)


def vault_startup_command_userpass(cluster):
    # enable user/password auth
    payload = {"type": "userpass"}
    send_post_request(cluster, "sys/auth/userpass", payload, 204)

    # update admins policy to manage secrets
    payload = {"policy": 'path "*" {capabilities = ["create", "read"]}'}
    send_post_request(cluster, "sys/policies/acl/admins", payload, 204)

    # create user
    payload = {"password": "test", "token_policies": ["admins"]}
    send_post_request(cluster, "auth/userpass/users/user1", payload, 204)

    # write secret
    payload = {
        "data": {
            "password": "test_password",
        }
    }
    send_post_request(cluster, "secret/data/username", payload)
