import requests
from helpers.cluster import ClickHouseCluster
import logging
import time


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
    # enable audit logging
    payload = {
        "type": "file",
        "options": {
            "file_path": "/vault/logs/audit.log",
            "log_raw": True,
            "hmac_accessor": False,
        },
    }
    send_post_request(cluster, "sys/audit/file-audit", payload, 204)

    # write secret
    payload = {
        "data": {
            "password": "test_password",
        }
    }
    send_post_request(cluster, "secret/data/username", payload)


def vault_startup_command_codecs(cluster):
    vault_startup_command(cluster)

    # write secret
    payload = {
        "data": {
            "key": "00112233445566778899AABBCCDDEEFF00112233445566778899AABBCCDDEEFF",
            "nonce_key": "ABCDEFABCDEFABCDEFABCDEF",
        }
    }
    send_post_request(cluster, "secret/data/aes_256", payload)


def update_default_policy(cluster):
    # update default policy to manage secrets
    payload = {"policy": 'path "*" {capabilities = ["create", "read"]}'}
    send_post_request(cluster, "sys/policies/acl/default", payload, 204)


def vault_startup_command_userpass(cluster):
    vault_startup_command(cluster)
    update_default_policy(cluster)

    # enable user/password auth
    payload = {"type": "userpass"}
    send_post_request(cluster, "sys/auth/userpass", payload, 204)

    # create user
    payload = {"password": "test", "token_policies": ["default"]}
    send_post_request(cluster, "auth/userpass/users/user1", payload, 204)


def read_cert(cert_file_path):
    with open(cert_file_path, "r") as f:
        return f.read().strip()


def vault_startup_command_cert(cluster):
    vault_startup_command(cluster)
    update_default_policy(cluster)

    # enable cert auth
    payload = {"type": "cert"}
    send_post_request(cluster, "sys/auth/cert", payload, 204)

    # create CA certificate role
    cluster.exec_in_container(
        cluster.hashicorp_vault_docker_id,
        [
            "vault",
            "write",
            "auth/cert/certs/client",
            "display_name=client",
            "policies=default",
            "certificate=@/vault/certs/client.crt",
        ],
    )
