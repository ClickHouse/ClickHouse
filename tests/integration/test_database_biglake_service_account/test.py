import base64
import json
import os
import time

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")
# Only `node` (where the mock runs) is allowed, so a token endpoint on any other host must be rejected.
restricted_node = cluster.add_instance(
    "restricted_node", main_configs=["configs/remote_url_allow_hosts.xml"]
)
# Gets a service account key in its server `<s3>` endpoint config at test time.
server_key_node = cluster.add_instance("server_key_node")

MOCK_PORT = 8939
SA_CLIENT_EMAIL = "tester@example-project.iam.gserviceaccount.com"
SA_TOKEN = "test-sa-token"
PROJECT = "test-project"
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# A throwaway key generated per run, used only for signing JWTs to the mock server.
PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
PRIVATE_KEY_PEM = PRIVATE_KEY.private_bytes(
    serialization.Encoding.PEM,
    serialization.PrivateFormat.PKCS8,
    serialization.NoEncryption(),
).decode()


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.dirname(__file__),
            [("biglake_mock_server.py", "node", MOCK_PORT)],
        )
        yield cluster
    finally:
        cluster.shutdown()


def mock_ctl(path):
    return cluster.exec_in_container(
        cluster.get_container_id("node"),
        ["curl", "-s", f"http://localhost:{MOCK_PORT}{path}"],
    )


def b64url_decode(data):
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def sql_str(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def service_account_key(client_email=SA_CLIENT_EMAIL, token_host="localhost"):
    return json.dumps(
        {
            "type": "service_account",
            "project_id": PROJECT,
            "client_email": client_email,
            "private_key": PRIVATE_KEY_PEM,
            "token_uri": f"http://{token_host}:{MOCK_PORT}/token",
        }
    )


def create_database(instance, name, key, catalog_host="localhost", extra_settings=""):
    return instance.query_and_get_answer_with_error(
        f"""
        CREATE DATABASE {name}
        ENGINE = DataLakeCatalog('http://{catalog_host}:{MOCK_PORT}/iceberg')
        SETTINGS catalog_type = 'biglake', warehouse = 'gs://bucket', google_project_id = '{PROJECT}',
            google_service_account_key = {sql_str(key)} {extra_settings}
        """,
        settings={"allow_database_iceberg": 1},
    )


def test_service_account_key_auth():
    mock_ctl("/__reset__")
    node.query("DROP DATABASE IF EXISTS biglake_sa")

    _, error = create_database(node, "biglake_sa", service_account_key())
    assert error == ""
    assert node.query("SHOW TABLES FROM biglake_sa") == ""

    stats = json.loads(mock_ctl("/__stats__"))

    # The token was minted once, from a JWT signed with the service account key.
    assert len(stats["assertions"]) == 1
    header_b64, claims_b64, signature_b64 = stats["assertions"][0].split(".")
    header = json.loads(b64url_decode(header_b64))
    claims = json.loads(b64url_decode(claims_b64))
    assert header == {"alg": "RS256", "typ": "JWT"}
    assert claims["iss"] == SA_CLIENT_EMAIL
    assert claims["scope"] == CLOUD_PLATFORM_SCOPE
    assert claims["aud"] == f"http://localhost:{MOCK_PORT}/token"
    assert claims["iat"] < claims["exp"] <= claims["iat"] + 3600
    # Raises InvalidSignature if the assertion was not signed with the key.
    PRIVATE_KEY.public_key().verify(
        b64url_decode(signature_b64),
        f"{header_b64}.{claims_b64}".encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )

    # Every catalog request carried the minted token and the project.
    requests = stats["catalog_requests"]
    assert {r["path"] for r in requests} >= {
        "/iceberg/v1/config",
        "/iceberg/v1/namespaces",
    }
    for request in requests:
        assert request["authorization"] == f"Bearer {SA_TOKEN}"
        assert request["user_project"] == PROJECT

    # The key is a secret.
    create_query = node.query("SHOW CREATE DATABASE biglake_sa")
    assert "google_service_account_key = \\'[HIDDEN]\\'" in create_query
    assert "PRIVATE KEY" not in create_query
    assert SA_CLIENT_EMAIL not in create_query

    node.query("DROP DATABASE biglake_sa")


def test_service_account_token_renewal():
    """An expired token is renewed with a new assertion signed by the key, and catalog requests keep working."""
    mock_ctl("/__reset__")
    # ClickHouse renews a token after 9/10 of its lifetime, i.e. after one second here.
    mock_ctl("/__set_expires_in__?seconds=2")
    node.query("DROP DATABASE IF EXISTS biglake_sa_renewal")
    _, error = create_database(node, "biglake_sa_renewal", service_account_key())
    assert error == ""

    deadline = time.monotonic() + 60
    while len(json.loads(mock_ctl("/__stats__"))["assertions"]) < 2:
        assert time.monotonic() < deadline, "the token was not renewed"
        assert node.query("SHOW TABLES FROM biglake_sa_renewal") == ""
        time.sleep(0.2)

    stats = json.loads(mock_ctl("/__stats__"))
    for assertion in stats["assertions"]:
        claims = json.loads(b64url_decode(assertion.split(".")[1]))
        assert claims["iss"] == SA_CLIENT_EMAIL
    for request in stats["catalog_requests"]:
        assert request["authorization"] == f"Bearer {SA_TOKEN}"

    node.query("DROP DATABASE biglake_sa_renewal")
    mock_ctl("/__reset__")


def test_token_endpoint_rejects_assertion():
    node.query("DROP DATABASE IF EXISTS biglake_sa_unknown")
    _, error = create_database(
        node,
        "biglake_sa_unknown",
        service_account_key(client_email="intruder@example.com"),
    )
    assert "AUTHENTICATION_FAILED" in error
    assert "Failed to obtain GCP access token" in error
    # The reason given by the token endpoint is part of the error.
    assert "unknown service account" in error


def test_token_endpoint_must_be_allowed():
    restricted_node.query("DROP DATABASE IF EXISTS biglake_sa_forbidden")
    _, error = create_database(
        restricted_node,
        "biglake_sa_forbidden",
        service_account_key(token_host="forbidden.example.com"),
        catalog_host="node",
    )
    assert "UNACCEPTABLE_URL" in error
    assert "forbidden.example.com" in error


def test_server_key_not_used_by_restricted_session():
    """A service account key of the server `<s3>` config stays the server's after an opt-in session creates a table:
    it follows a rotation of the server config, and a restricted session never mints a token with it.
    """
    data_url = f"http://node:{MOCK_PORT}/data"
    rotated_email = "rotated@example-project.iam.gserviceaccount.com"
    user_email = "user@example-project.iam.gserviceaccount.com"
    user_key = service_account_key(client_email=user_email, token_host="node")

    def set_server_key(server_key):
        server_key_node.replace_config(
            "/etc/clickhouse-server/config.d/server_s3_service_account.xml",
            f"""<clickhouse>
    <s3>
        <gcs_with_server_key>
            <endpoint>{data_url}/</endpoint>
            <http_client>gcp_oauth</http_client>
            <google_service_account_key>{server_key}</google_service_account_key>
        </gcs_with_server_key>
    </s3>
    <named_collections>
        <user_key_collection>
            <url>{data_url}/user.tsv</url>
            <http_client>gcp_oauth</http_client>
            <google_service_account_key>{user_key}</google_service_account_key>
        </user_key_collection>
    </named_collections>
</clickhouse>""",
        )
        server_key_node.query("SYSTEM RELOAD CONFIG")

    def issuers_of_new_assertions(query, allow_server_credentials):
        mock_ctl("/__reset__")
        _, error = server_key_node.query_and_get_answer_with_error(
            query,
            settings={
                "s3_allow_server_credentials_in_user_queries": allow_server_credentials
            },
        )
        assertions = json.loads(mock_ctl("/__stats__"))["assertions"]
        return [
            json.loads(b64url_decode(a.split(".")[1]))["iss"] for a in assertions
        ], error

    set_server_key(service_account_key(token_host="node"))
    assert (
        server_key_node.query(
            "SELECT count() FROM system.named_collections WHERE name = 'user_key_collection'"
        )
        == "1\n"
    )

    server_key_node.query("DROP TABLE IF EXISTS t_server_key")
    server_key_node.query(
        f"CREATE TABLE t_server_key (x UInt8) ENGINE = S3('{data_url}/server.tsv', 'TSV')",
        settings={"s3_allow_server_credentials_in_user_queries": 1},
    )

    # The opt-in session uses the server key.
    issuers, _ = issuers_of_new_assertions("SELECT * FROM t_server_key", 1)
    assert SA_CLIENT_EMAIL in issuers

    # The table follows a rotation of the server key, because it did not supply the key itself.
    set_server_key(service_account_key(client_email=rotated_email, token_host="node"))
    issuers, _ = issuers_of_new_assertions("SELECT * FROM t_server_key", 1)
    assert rotated_email in issuers
    assert SA_CLIENT_EMAIL not in issuers

    # A restricted session is refused and never mints a token with the server key, also after the reload.
    issuers, error = issuers_of_new_assertions("SELECT * FROM t_server_key", 0)
    assert issuers == []
    assert "ACCESS_DENIED" in error

    # A key supplied by a named collection is explicit, so a restricted session still uses it.
    issuers, error = issuers_of_new_assertions(
        "SELECT * FROM s3(user_key_collection, format = 'TSV', structure = 'x UInt8')",
        0,
    )
    assert "ACCESS_DENIED" not in error
    assert issuers and set(issuers) == {user_email}

    server_key_node.query("DROP TABLE t_server_key")
