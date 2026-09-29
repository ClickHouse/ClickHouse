import base64
import json
import os

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


def test_token_endpoint_rejects_assertion():
    node.query("DROP DATABASE IF EXISTS biglake_sa_unknown")
    _, error = create_database(
        node,
        "biglake_sa_unknown",
        service_account_key(client_email="intruder@example.com"),
    )
    assert "AUTHENTICATION_FAILED" in error
    assert "Failed to obtain GCP access token" in error


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
