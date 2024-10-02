import os
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/verify_static_key.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
    # We actually don't need minio, but we need to run dummy resolver
    # (a shortcut not to change cluster.py in a more unclear way, TBC later).
)
client = cluster.add_instance(
    "client",
)


def run_jwks_server():
    script_dir = os.path.join(os.path.dirname(__file__), "jwks_server")
    start_mock_servers(
        cluster,
        script_dir,
        [
            ("server.py", "resolver", "8080"),
        ],
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        run_jwks_server()
        yield cluster
    finally:
        cluster.shutdown()


def curl_with_jwt(token, ip, https=False):
    http_prefix = "https" if https else "http"
    curl = f'curl -H "X-ClickHouse-JWT-Token: Bearer {token}" "{http_prefix}://{ip}:8123/?query=SELECT%20currentUser()"'
    return curl


# See helpers/ directory if you need to re-create tokens (or understand how they are created)
def test_static_key(started_cluster):
    res = client.exec_in_container(
        [
            "bash",
            "-c",
            curl_with_jwt(
                token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJqd3RfdXNlciJ9."
                "kfivQ8qD_oY0UvihydeadD7xvuiO3zSmhFOc_SGbEPQ",
                ip=cluster.get_instance_ip(instance.name),
            ),
        ]
    )
    assert res == "jwt_user\n"


def test_static_jwks(started_cluster):
    res = client.exec_in_container(
        [
            "bash",
            "-c",
            curl_with_jwt(
                token="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6Im15a2lkIn0."
                "eyJzdWIiOiJqd3RfdXNlciIsImlzcyI6InRlc3RfaXNzIn0."
                "CUioyRc_ms75YWkUwvPgLvaVk2Wmj8RzgqDALVd9LWUzCL5aU4yc_YaA3qnG_NoHd0uUF4FUjLxiocRoKNEgsE2jj7g_"
                "wFMC5XHSHuFlfIZjovObXQEwGcKpXO2ser7ANu3k2jBC2FMpLfr_sZZ_GYSnqbp2WF6-l0uVQ0AHVwOy4x1Xkawiubkg"
                "W2I2IosaEqT8QNuvvFWLWc1k-dgiNp8k6P-K4D4NBQub0rFlV0n7AEKNdV-_AEzaY_IqQT0sDeBSew_mdR0OH_N-6-"
                "FmWWIroIn2DQ7pq93BkI7xdkqnxtt8RCWkCG8JLcoeJt8sHh7uTKi767loZJcPPNaxKA",
                ip=cluster.get_instance_ip(instance.name),
            ),
        ]
    )
    assert res == "jwt_user\n"


def test_jwks_server(started_cluster):
    res = client.exec_in_container(
        [
            "bash",
            "-c",
            curl_with_jwt(
                token="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiIsImtpZCI6Im15a2lkIn0."
                      "eyJzdWIiOiJqd3RfdXNlciIsImlzcyI6InRlc3RfaXNzIn0.MjegqrrVyrMMpkxIM-J_q-"
                      "Sw68Vk5xZuFpxecLLMFs5qzvnh0jslWtyRfi-ANJeJTONPZM5m0yP1ITt8BExoHWobkkR11bXz0ylYEIOgwxqw"
                      "36XhL2GkE17p-wMvfhCPhGOVL3b7msDRUKXNN48aAJA-NxRbQFhMr-eEx3HsrZXy17Qc7z-"
                      "0dINe355kzAInGp6gMk3uksAlJ3vMODK8jE-WYFqXusr5GFhXubZXdE2mK0mIbMUGisOZhZLc4QVwvUsYDLBCgJ2RHr5vm"
                      "jp17j_ZArIedUJkjeC4o72ZMC97kLVnVw94QJwNvd4YisxL6A_mWLTRq9FqNLD4HmbcOQ",
                ip=cluster.get_instance_ip(instance.name),
            ),
        ]
    )
    assert res == "jwt_user\n"
