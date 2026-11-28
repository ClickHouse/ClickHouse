import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/overrides.yaml"],
    user_configs=["users/overrides.yaml"],
    stay_alive=True,
)

CONFIG = "/etc/clickhouse-server/config.d/overrides.yaml"
ADMIN = {"user": "admin", "password": "1w2swhb1"}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def set_policy(allowed):
    value = 1 if allowed else 0
    for setting in ("allow_no_password", "allow_plaintext_password"):
        node.replace_in_config(CONFIG, f"{setting}: {1 - value}", f"{setting}: {value}")
    node.restart_clickhouse()


def show_create(user):
    return node.query(f"SHOW CREATE USER {user}", **ADMIN).strip()


def test_alter_user_checks_only_new_authentication_methods(start_cluster):
    # Set up users while every authentication type is still allowed.
    set_policy(allowed=True)
    node.query("DROP USER IF EXISTS u_plaintext, u_no_password, u_sha256, u_renamed", **ADMIN)
    node.query("CREATE USER u_plaintext IDENTIFIED WITH plaintext_password BY 'x'", **ADMIN)
    node.query("CREATE USER u_no_password IDENTIFIED WITH no_password", **ADMIN)
    node.query("CREATE USER u_sha256 IDENTIFIED WITH sha256_password BY 'x'", **ADMIN)

    set_policy(allowed=False)

    # New disallowed methods are rejected by `ALTER`, as they are by `CREATE`.
    sha256_before = show_create("u_sha256")
    for query in (
        "ALTER USER u_sha256 IDENTIFIED WITH no_password",
        "ALTER USER u_sha256 IDENTIFIED WITH plaintext_password BY 'y'",
        "ALTER USER u_sha256 ADD IDENTIFIED WITH plaintext_password BY 'y'",
    ):
        error = node.query_and_get_error(query, **ADMIN)
        assert "BAD_ARGUMENTS" in error and "is not allowed" in error, query
    assert show_create("u_sha256") == sha256_before

    # Methods the user already has are not re-checked, so unrelated alters keep working.
    node.query("ALTER USER u_plaintext SETTINGS readonly = 0", **ADMIN)
    node.query("ALTER USER u_no_password SETTINGS readonly = 0", **ADMIN)
    node.query("ALTER USER u_no_password RENAME TO u_renamed", **ADMIN)
    assert show_create("u_plaintext") == "CREATE USER u_plaintext IDENTIFIED WITH plaintext_password SETTINGS readonly = 0"
    assert show_create("u_renamed") == "CREATE USER u_renamed IDENTIFIED WITH no_password SETTINGS readonly = 0"

    # Adding an allowed method to a user that holds a grandfathered one works.
    node.query("ALTER USER u_plaintext ADD IDENTIFIED WITH sha256_password BY 'y'", **ADMIN)
    assert "sha256_password" in show_create("u_plaintext")

    # Replacing a grandfathered method with an allowed one works too.
    node.query("ALTER USER u_renamed IDENTIFIED WITH sha256_password BY 'y'", **ADMIN)
    assert "IDENTIFIED WITH sha256_password" in show_create("u_renamed")

    # `CREATE` is still validated, including the implicit `no_password`.
    assert "is not allowed" in node.query_and_get_error("CREATE USER u_new IDENTIFIED WITH no_password", **ADMIN)
    assert "is not allowed" in node.query_and_get_error("CREATE USER u_new IDENTIFIED WITH plaintext_password BY 'x'", **ADMIN)

    node.query("DROP USER IF EXISTS u_plaintext, u_renamed, u_sha256", **ADMIN)
