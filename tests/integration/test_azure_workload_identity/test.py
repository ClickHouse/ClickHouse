import json
import os
import time

import pytest
import requests
import urllib3

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

TENANT_ID = "11111111-1111-1111-1111-111111111111"
CLIENT_ID = "22222222-2222-2222-2222-222222222222"
FEDERATED_TOKEN = "dummy-federated-token"
TOKEN_PORT = 8443

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/disable_cert_verification.xml"],
    env_variables={
        "AZURE_TENANT_ID": TENANT_ID,
        "AZURE_CLIENT_ID": CLIENT_ID,
        "AZURE_FEDERATED_TOKEN_FILE": "/tmp/azure_federated_token",
        "AZURE_AUTHORITY_HOST": f"https://127.0.0.1:{TOKEN_PORT}/",
    },
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        container_id = cluster.get_container_id("node")
        cluster.copy_file_to_container(
            container_id,
            os.path.join(SCRIPT_DIR, "mock_token_server.py"),
            "mock_token_server.py",
        )
        cluster.exec_in_container(
            container_id,
            [
                "bash",
                "-c",
                f"echo -n {FEDERATED_TOKEN} > /tmp/azure_federated_token",
            ],
        )
        cluster.exec_in_container(
            container_id,
            [
                "bash",
                "-c",
                f"python3 mock_token_server.py {TOKEN_PORT} "
                "> /var/log/clickhouse-server/mock_token_server.log 2>&1",
            ],
            detach=True,
        )

        for _ in range(60):
            ping = cluster.exec_in_container(
                container_id,
                ["curl", "-sk", f"https://127.0.0.1:{TOKEN_PORT}/"],
                nothrow=True,
            )
            if ping == "OK":
                break
            time.sleep(1)
        else:
            raise Exception("Mock token server did not start")

        yield cluster
    finally:
        cluster.shutdown()


def test_token_request_goes_through_http_transport(started_cluster):
    """`WorkloadIdentityCredential` must fetch a token from the authority host
    through the HTTP transport of the Azure SDK. The query is expected to fail
    later, on the request to the (unreachable) storage endpoint."""

    error = node.query_and_get_error(
        "SELECT * FROM azureBlobStorage('https://localhost:1', 'cont', 'data.csv', 'CSV', 'auto', 'c UInt64') "
        "SETTINGS s3_allow_server_credentials_in_user_queries = 1"
    )
    assert "AuthenticationException" not in error

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    node_ip = started_cluster.get_instance_ip("node")
    hits = json.loads(
        requests.get(f"https://{node_ip}:{TOKEN_PORT}/hits", verify=False).text
    )

    assert len(hits) >= 1
    for hit in hits:
        assert hit["path"] == f"/{TENANT_ID}/oauth2/v2.0/token"
        assert FEDERATED_TOKEN in hit["body"]


def get_storage_hits(started_cluster):
    node_ip = started_cluster.get_instance_ip("node")
    return json.loads(
        requests.get(f"https://{node_ip}:{TOKEN_PORT}/storage_hits", verify=False).text
    )


def test_table_created_with_opt_in_serves_no_restricted_session(started_cluster):
    """A table that uses the server's identity follows the restriction of the accessing session, as for S3: a
    restricted session is refused before any request, and the opt-in works again afterwards."""

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    allow = {"s3_allow_server_credentials_in_user_queries": 1}
    node.query("DROP TABLE IF EXISTS t_opt_in SYNC")
    node.query(
        f"CREATE TABLE t_opt_in (c UInt64) ENGINE = AzureBlobStorage('https://127.0.0.1:{TOKEN_PORT}/account', 'cont', 'data.csv', 'CSV')",
        settings=allow,
    )

    before = len(get_storage_hits(started_cluster))
    error = node.query_and_get_error("SELECT * FROM t_opt_in", settings={"azure_sdk_max_retries": 0})
    assert "ACCESS_DENIED" in error
    assert get_storage_hits(started_cluster)[before:] == []

    # Entrypoints that do not refresh the client, such as `ALTER` and `OPTIMIZE` of a data lake, are checked as well.
    for query in ["OPTIMIZE TABLE t_opt_in", "ALTER TABLE t_opt_in ADD COLUMN d UInt64"]:
        before = len(get_storage_hits(started_cluster))
        assert "ACCESS_DENIED" in node.query_and_get_error(query, settings={"azure_sdk_max_retries": 0})
        assert get_storage_hits(started_cluster)[before:] == []

    before = len(get_storage_hits(started_cluster))
    node.query_and_get_error("SELECT * FROM t_opt_in", settings={**allow, "azure_sdk_max_retries": 0})
    allowed_hits = get_storage_hits(started_cluster)[before:]
    assert any(hit["authorization"] for hit in allowed_hits)

    node.query("DROP TABLE t_opt_in SYNC")


def test_truncate_follows_the_session(started_cluster):
    """`TRUNCATE` uses the client of the truncating session, not one an opted-in session left: a restricted session is
    refused before any delete is sent."""

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    allow = {"s3_allow_server_credentials_in_user_queries": 1}
    node.query("DROP TABLE IF EXISTS t_truncate SYNC")
    node.query(
        f"CREATE TABLE t_truncate (c UInt64) ENGINE = AzureBlobStorage('https://127.0.0.1:{TOKEN_PORT}/account', 'cont', 'data.csv', 'CSV')",
        settings=allow,
    )

    before = len(get_storage_hits(started_cluster))
    error = node.query_and_get_error("TRUNCATE TABLE t_truncate", settings={"azure_sdk_max_retries": 0})
    assert "ACCESS_DENIED" in error
    assert get_storage_hits(started_cluster)[before:] == []

    node.query("DROP TABLE t_truncate SYNC")
