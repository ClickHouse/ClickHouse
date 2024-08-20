import os
import pytest
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/verify_static_key.xml"],
    user_configs=["configs/users.xml"],
)
client = cluster.add_instance(
    "client",
    main_configs=["configs/verify_static_key.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def curl_with_jwt(token, ip, https=False):
    http_prefix = "https" if https else "http"
    curl = f'curl -H "X-ClickHouse-JWT-Token: Bearer {token}" "{http_prefix}://{ip}:8123/?query=SELECT%20currentUser()"'
    return curl


# See helpers.py if you need to re-create tokens
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
