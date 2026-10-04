import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Without `macros` the harness defines neither `{shard}` nor `{replica}`; a remote database disk would add both.
node_without_macros = cluster.add_instance(
    "node_without_macros",
    main_configs=["configs/default_replica_path.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
)
node_with_macros = cluster.add_instance(
    "node_with_macros",
    main_configs=["configs/default_replica_path.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
    macros={"shard": "config_shard", "replica": "config_replica"},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def replica_path(node, table):
    return node.query(
        f"SELECT zookeeper_path, replica_name FROM system.replicas "
        f"WHERE database = 'rdb' AND table = '{table}'"
    ).strip()


def test_table_loads_after_restart_without_config_macros(started_cluster):
    node_without_macros.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'db_shard_a', 'db_replica_a')"
    )
    node_with_macros.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/test/rdb', 'db_shard_b', 'db_replica_b')"
    )
    node_without_macros.query(
        "CREATE TABLE rdb.t (x Int32) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    assert_eq_with_retry(
        node_with_macros,
        "SELECT count() FROM system.tables WHERE database = 'rdb' AND name = 't'",
        "1",
    )

    for node in (node_without_macros, node_with_macros):
        create_query = node.query("SHOW CREATE TABLE rdb.t")
        assert "{shard}" in create_query and "{replica}" in create_query

    node_without_macros.query("INSERT INTO rdb.t VALUES (1), (2), (3)")
    node_with_macros.query("INSERT INTO rdb.t VALUES (10), (20)")

    # A config macro wins over the database argument.
    path_without_macros = replica_path(node_without_macros, "t")
    path_with_macros = replica_path(node_with_macros, "t")
    assert path_without_macros.endswith("/db_shard_a\tdb_replica_a")
    assert path_with_macros.endswith("/config_shard\tconfig_replica")

    node_without_macros.restart_clickhouse()
    node_with_macros.restart_clickhouse()

    assert node_without_macros.query("SELECT count() FROM rdb.t") == "3\n"
    assert node_with_macros.query("SELECT count() FROM rdb.t") == "2\n"

    assert replica_path(node_without_macros, "t") == path_without_macros
    assert replica_path(node_with_macros, "t") == path_with_macros

    node_without_macros.query("INSERT INTO rdb.t VALUES (4)")
    assert node_without_macros.query("SELECT count() FROM rdb.t") == "4\n"

    # The database's DDL worker came back after the restart.
    node_without_macros.query(
        "CREATE TABLE rdb.t2 (x Int32) ENGINE = ReplicatedMergeTree ORDER BY x"
    )
    assert_eq_with_retry(
        node_with_macros,
        "SELECT count() FROM system.tables WHERE database = 'rdb' AND name = 't2'",
        "1",
    )
    assert replica_path(node_without_macros, "t2").endswith(
        "/db_shard_a\tdb_replica_a"
    )
    assert replica_path(node_with_macros, "t2").endswith(
        "/config_shard\tconfig_replica"
    )


def set_config_macros(node, macros):
    # The harness writes the shard/replica macros into conf.d/macros.xml, alongside {instance};
    # rewrite that same file so the change actually removes them.
    full = {"instance": node.name}
    full.update(macros)
    node.replace_config(
        "/etc/clickhouse-server/conf.d/macros.xml",
        "<clickhouse><macros>"
        + "".join(f"<{k}>{v}</{k}>" for k, v in full.items())
        + "</macros></clickhouse>",
    )


def test_table_created_with_config_macros_loads_without_them(started_cluster):
    # `t` was created above while the config defined the macros, so its stored path still holds `{shard}`.
    path_with_macros = replica_path(node_with_macros, "t")
    assert path_with_macros.endswith("/config_shard\tconfig_replica")

    set_config_macros(node_with_macros, {})
    node_with_macros.restart_clickhouse()

    # Bound to the database arguments now: no replica exists on that path, so the table is read-only
    # with its local data intact, instead of failing to load.
    assert replica_path(node_with_macros, "t").endswith("/db_shard_b\tdb_replica_b")
    assert (
        node_with_macros.query(
            "SELECT is_readonly FROM system.replicas WHERE database = 'rdb' AND table = 't'"
        )
        == "1\n"
    )
    assert node_with_macros.query("SELECT count() FROM rdb.t") == "2\n"
    assert "TABLE_IS_READ_ONLY" in node_with_macros.query_and_get_error(
        "INSERT INTO rdb.t VALUES (30)"
    )

    # The configured macros back, the table is its old replica again.
    set_config_macros(
        node_with_macros, {"shard": "config_shard", "replica": "config_replica"}
    )
    node_with_macros.restart_clickhouse()

    assert replica_path(node_with_macros, "t") == path_with_macros
    node_with_macros.query("INSERT INTO rdb.t VALUES (30)")
    assert node_with_macros.query("SELECT count() FROM rdb.t") == "3\n"
