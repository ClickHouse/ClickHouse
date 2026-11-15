import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `node_on` enables `EXECUTE AS ... SETTINGS` (with `settings_constraints_replace_previous = false`),
# `node_off` runs with the default from `config.xml`, `node_old` is a release without the feature.
node_on = cluster.add_instance(
    "node_on",
    main_configs=["configs/execute_as_settings.xml", "configs/remote_servers.xml"],
)
node_off = cluster.add_instance(
    "node_off", main_configs=["configs/remote_servers.xml"]
)
node_old = cluster.add_instance(
    "node_old",
    main_configs=["configs/remote_servers.xml"],
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)

FILL = "EXECUTE AS qu SETTINGS custom_tenant = 'acme'"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        for node in (node_on, node_off, node_old):
            node.query(
                """
                CREATE TABLE t (tenant String, v UInt8) ENGINE = MergeTree ORDER BY v;
                INSERT INTO t VALUES ('acme', 1), ('acme', 2), ('other', 3);
                CREATE SETTINGS PROFILE p_lock SETTINGS custom_tenant CONST;
                CREATE SETTINGS PROFILE p_other SETTINGS custom_tenant = 'other';
                CREATE SETTINGS PROFILE p_writable SETTINGS custom_tenant WRITABLE;
                CREATE SETTINGS PROFILE p_valueless SETTINGS custom_tenant CONST;
                CREATE USER gw;
                CREATE USER qu SETTINGS PROFILE p_lock;
                CREATE USER qw;
                GRANT SELECT ON default.* TO qu, qw;
                CREATE ROW POLICY rp ON t USING tenant = getSetting('custom_tenant') TO qu;
                CREATE ROW POLICY rp_qw ON t USING 1 TO qw;
                """
            )
        for node in (node_on, node_off):
            node.query("GRANT IMPERSONATE ON qu TO gw; GRANT IMPERSONATE ON qw TO gw")
        node_on.query(
            """
            CREATE TABLE d_off AS t ENGINE = Distributed(c_sec_off, default, t);
            CREATE TABLE d_old AS t ENGINE = Distributed(c_sec_old, default, t);
            """
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_disabled_by_default():
    for query in (f"{FILL} SELECT 1", FILL):
        assert "SUPPORT_IS_DISABLED" in node_off.query_and_get_error(query, user="gw")
    assert node_off.query("EXECUTE AS qu SELECT currentUser()", user="gw") == "qu\n"
    # A value set before the session switches to `qu` does not stay in its `CONST` setting either.
    assert "SETTING_CONSTRAINT_VIOLATION" in node_off.query_and_get_error(
        "SET custom_tenant = 'acme'; EXECUTE AS qu; SELECT getSetting('custom_tenant')",
        user="gw",
    )


def test_set_profile_without_replace_previous():
    for profile in ("p_writable", "p_other"):
        assert "SETTING_CONSTRAINT_VIOLATION" in node_on.query_and_get_error(
            f"{FILL}; SET profile = '{profile}'", user="gw"
        )
    assert (
        node_on.query(
            f"{FILL}; SET profile = 'p_valueless'; SELECT getSetting('custom_tenant')",
            user="gw",
        )
        == "acme\n"
    )


def test_remote_node_with_the_feature_disabled():
    error = node_on.query_and_get_error(
        f"{FILL} SELECT count() FROM d_off",
        user="gw",
        settings={"prefer_localhost_replica": 0},
    )
    assert "SUPPORT_IS_DISABLED" in error and "node_off" in error


def test_remote_node_of_an_older_version():
    assert "UNSUPPORTED_METHOD" in node_on.query_and_get_error(
        f"{FILL} SELECT count() FROM d_old",
        user="gw",
        settings={"prefer_localhost_replica": 0},
    )
    assert (
        node_on.query(
            "EXECUTE AS qw SELECT count() FROM d_old",
            user="gw",
            settings={"prefer_localhost_replica": 0},
        )
        == "6\n"
    )
