import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The node starts with plaintext passwords allowed, so an ambiguous user can be created: a strong (always allowed)
# authentication method plus a plaintext method that shares the same secret but carries a narrower `GRANTS` clause.
# The plaintext method is then disabled via `allow_plaintext_password = 0` and the server is restarted. A disabled
# method must be ignored entirely - both in the primary authentication loop and in the fail-close ambiguity scan -
# so it must not be able to narrow the rights of the still-allowed method's login.
#
# The `default` user of the integration framework is defined with a plaintext password in `users.xml`, which would
# make the server refuse to start once `allow_plaintext_password = 0`, so it is switched to `no_password` here
# (`allow_no_password` stays enabled).
node = cluster.add_instance(
    "node",
    main_configs=["configs/allow_plaintext.yaml"],
    user_configs=["users/default_no_password.yaml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_disabled_method_does_not_narrow_allowed_login(start_cluster):
    node.query("DROP USER IF EXISTS u_disabled_grants")
    # The table must survive the server restart below, so `Memory` is not suitable here.
    node.query("CREATE TABLE IF NOT EXISTS default.t_disabled_grants (x UInt64) ENGINE = MergeTree ORDER BY x")
    node.query("INSERT INTO default.t_disabled_grants VALUES (1)")

    # The `sha256_password` method is always allowed and carries no `GRANTS` clause (so on its own it is unrestricted).
    # The `plaintext_password` method shares the same secret but is limited to a different table, so if it participates
    # in the fail-close ambiguity scan the session is narrowed away from t_disabled_grants.
    node.query("CREATE USER u_disabled_grants IDENTIFIED WITH sha256_password BY 'shared', plaintext_password BY 'shared' GRANTS (SELECT ON default.other_table)")
    node.query("GRANT SELECT ON default.t_disabled_grants TO u_disabled_grants")

    # While plaintext passwords are allowed, the plaintext method participates in the ambiguity scan and narrows the
    # login to its own disjoint grants, so t_disabled_grants is denied. This confirms the ambiguity scan is active.
    assert "ACCESS_DENIED" in node.query_and_get_error(
        "SELECT x FROM default.t_disabled_grants",
        user="u_disabled_grants",
        password="shared",
    )

    # Disable plaintext passwords and restart. The stored plaintext method must now be ignored completely.
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/allow_plaintext.yaml",
        "allow_plaintext_password: 1",
        "allow_plaintext_password: 0",
    )
    node.restart_clickhouse()

    # The login still uses the allowed sha256_password method. The disabled plaintext method must not fold its
    # narrower GRANTS into the session, so t_disabled_grants becomes readable again with the same credential.
    assert (
        node.query(
            "SELECT x FROM default.t_disabled_grants",
            user="u_disabled_grants",
            password="shared",
        )
        == "1\n"
    )

    node.query("DROP USER u_disabled_grants")
    node.query("DROP TABLE default.t_disabled_grants")
