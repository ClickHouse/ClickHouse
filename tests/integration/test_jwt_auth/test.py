import os
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/validators.xml"],
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
    curl = f'curl -H "Authorization: Bearer {token}" "{http_prefix}://{ip}:8123/?query=SELECT%20currentUser()"'
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


def test_jwks_server_ec_es384(started_cluster):
    res = client.exec_in_container(
        [
            "bash",
            "-c",
            curl_with_jwt(
                token="eyJhbGciOiJFUzM4NCIsImtpZCI6ImVjbXlraWQiLCJ0eXAiOiJKV1QifQ."
                      "eyJzdWIiOiJqd3RfdXNlciIsImlzcyI6InRlc3RfaXNzIn0."
                      "3iGUcKfc07oLN4XmBA6BJSGSfu7cBsdQ6KAFh1sV64rWYkVL5VzYlAskHaWZ4R9hR3QK0Bv0EPjia8Vo-xdN9jS7-fVB7RF0"
                      "rGvbTOIuxE-yDumCyji3MYoLpcbOVasU",
                ip=cluster.get_instance_ip(instance.name),
            ),
        ]
    )
    assert res == "jwt_user\n"


# Helper: request `SELECT currentUser()` over HTTP with the given bearer token
# and return the body. Caller decides whether to assert on the username or on
# rejection (rejected requests return a non-username error body).
def http_select_current_user(token: str) -> str:
    return client.exec_in_container(
        [
            "bash",
            "-c",
            curl_with_jwt(token=token, ip=cluster.get_instance_ip(instance.name)),
        ]
    )


def make_token(payload: dict, secret: str) -> str:
    """Sign an HS256 JWT with the given secret. Matches the secrets configured
    for `single_key_processor` (`my_secret`) and `another_single_key_processor`
    (`other_secret`) in `configs/validators.xml`."""
    import jwt
    return jwt.encode(payload, secret, algorithm="HS256")


def test_sql_create_jwt_user_with_processor_pin(started_cluster):
    """SQL `CREATE USER ... IDENTIFIED WITH jwt PROCESSOR '<name>'` actually
    pins the auth path: a token that validates against a different processor
    in the same chain must NOT authenticate the SQL-pinned user. Without the
    pin the iterate-all-processors auto-discovery branch would happily accept
    either token (this is the H-22 / H-14 bypass surface)."""

    instance.query(
        "CREATE USER OR REPLACE sql_jwt_user IDENTIFIED WITH jwt PROCESSOR 'single_key_processor'"
    )

    # Round-trip: SHOW CREATE USER must emit the PROCESSOR clause we just set.
    # `TSVRaw` is used so single quotes in the SQL literal are not TSV-escaped.
    show = instance.query("SHOW CREATE USER sql_jwt_user FORMAT TSVRaw").strip()
    assert "PROCESSOR 'single_key_processor'" in show, show
    assert "CLAIMS" not in show, show

    token_my = make_token({"sub": "sql_jwt_user"}, "my_secret")
    token_other = make_token({"sub": "sql_jwt_user"}, "other_secret")

    # Pinned processor accepts the my_secret-signed token.
    assert http_select_current_user(token_my) == "sql_jwt_user\n"

    # The other_secret-signed token validates fine against
    # `another_single_key_processor`, but the user is pinned to
    # `single_key_processor` -- the pin must reject it.
    rejected = http_select_current_user(token_other)
    assert "AUTHENTICATION_FAILED" in rejected, rejected

    # Re-pin via ALTER and the relationship inverts.
    instance.query(
        "ALTER USER sql_jwt_user IDENTIFIED WITH jwt PROCESSOR 'another_single_key_processor'"
    )
    assert http_select_current_user(token_other) == "sql_jwt_user\n"
    rejected = http_select_current_user(token_my)
    assert "AUTHENTICATION_FAILED" in rejected, rejected

    instance.query("DROP USER sql_jwt_user")


def test_sql_create_jwt_user_with_claims(started_cluster):
    """`CLAIMS '<json>'` must be enforced for SQL-declared JWT users: a token
    that is valid against the pinned processor but lacks the required claim
    must be rejected, and a token that has the claim must be accepted."""

    instance.query(
        "CREATE USER OR REPLACE sql_jwt_claims_user "
        "IDENTIFIED WITH jwt PROCESSOR 'single_key_processor' "
        "CLAIMS '{\"role\":\"admin\"}'"
    )

    show = instance.query("SHOW CREATE USER sql_jwt_claims_user FORMAT TSVRaw").strip()
    assert "PROCESSOR 'single_key_processor'" in show, show
    assert "CLAIMS '{\"role\":\"admin\"}'" in show, show

    # Token signed with the pinned processor's secret but no `role` claim:
    # processor accepts, per-user CLAIMS rejects.
    token_no_claim = make_token({"sub": "sql_jwt_claims_user"}, "my_secret")
    rejected = http_select_current_user(token_no_claim)
    assert "AUTHENTICATION_FAILED" in rejected, rejected

    # Token with the required claim: both gates pass.
    token_with_claim = make_token(
        {"sub": "sql_jwt_claims_user", "role": "admin"}, "my_secret"
    )
    assert http_select_current_user(token_with_claim) == "sql_jwt_claims_user\n"

    instance.query("DROP USER sql_jwt_claims_user")


def test_sql_jwt_user_no_pin_uses_auto_discovery(started_cluster):
    """Without `PROCESSOR`, the SQL JWT user falls back to auto-discovery: any
    configured processor that validates the token will be accepted. This is
    the documented behavior for users who explicitly chose not to pin."""

    instance.query("CREATE USER OR REPLACE sql_jwt_unpinned IDENTIFIED WITH jwt")

    show = instance.query("SHOW CREATE USER sql_jwt_unpinned FORMAT TSVRaw").strip()
    assert "PROCESSOR" not in show, show

    # Both tokens (each valid against a different processor) authenticate the
    # same unpinned SQL user.
    for secret in ("my_secret", "other_secret"):
        token = make_token({"sub": "sql_jwt_unpinned"}, secret)
        assert http_select_current_user(token) == "sql_jwt_unpinned\n"

    instance.query("DROP USER sql_jwt_unpinned")
