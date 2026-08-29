# A table whose persisted DDL contains `toTime` composed with date arithmetic (written when the
# legacy meaning was the default) must be loadable after an upgrade by setting `use_legacy_to_time`
# in the server default profile: the metadata loader has no query context, so the profile is the
# only place the legacy remap can read from.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/async_load.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

LEGACY_USERS_XML = """
<clickhouse>
    <profiles>
        <default>
            <use_legacy_to_time>1</use_legacy_to_time>
        </default>
    </profiles>
</clickhouse>
"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_legacy_totime_metadata_load(started_cluster):
    node.query(
        "CREATE TABLE default.t_ttl (id UInt32, dt DateTime('UTC'), "
        "v UInt32 TTL toTimeWithFixedDate(dt) + INTERVAL 100 YEAR) "
        "ENGINE = MergeTree ORDER BY id"
    )
    node.query("INSERT INTO default.t_ttl VALUES (1, '2024-01-01 00:10:00', 7)")

    # Rewrite the stored definition to the raw `toTime` spelling, as written by 25.6-26.6 servers.
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            "sed -i 's/toTimeWithFixedDate/toTime/' "
            "$(grep -rl toTimeWithFixedDate /var/lib/clickhouse --include='*.sql')",
        ]
    )
    node.start_clickhouse()

    # At the new default the definition does not resolve and the table fails to load.
    assert "ILLEGAL_TYPE_OF_ARGUMENT" in node.query_and_get_error(
        "SELECT count() FROM default.t_ttl"
    )

    # The default profile setting heals the load.
    node.replace_config("/etc/clickhouse-server/users.d/users.xml", LEGACY_USERS_XML)
    node.restart_clickhouse()

    assert node.query("SELECT count() FROM default.t_ttl").strip() == "1"
    assert "toTime(dt)" in node.query("SHOW CREATE TABLE default.t_ttl")

    node.query("INSERT INTO default.t_ttl VALUES (2, '2024-01-02 00:10:00', 8)")
    assert node.query("SELECT count() FROM default.t_ttl").strip() == "2"

    node.query("DROP TABLE default.t_ttl")
