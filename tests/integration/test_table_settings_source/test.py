import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `system.table_settings.source` for settings a server config section assigns. Most of that is covered
# by `05058_settings_source_attribution`, which runs `clickhouse-local` with a config file. What needs a
# real server with Keeper is the `ReplicatedMergeTree` family, which reads its own
# `<replicated_merge_tree>` section into a separate cached baseline. The exact set of settings reported
# as `config` is checked here too, because a server's config includes the harness's own sections.
node = cluster.add_instance(
    "node",
    main_configs=["configs/merge_tree_settings.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def source_of(setting, table="t"):
    return node.query(
        f"SELECT source FROM system.table_settings "
        f"WHERE database = currentDatabase() AND table = '{table}' AND name = '{setting}'"
    ).strip()


def test_config_assignment_is_reported(started_cluster):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")

    # A setting the config does not mention at all.
    assert source_of("merge_max_block_size_bytes") == "default"

    # Nothing beyond what a config section assigned is attributed to one, so the reporting does not
    # over-apply. Two of these come from this test's config and two from the integration harness's
    # own `helpers/0_common_instance_config.xml`, which sets them for every instance.
    assert node.query(
        "SELECT name FROM system.table_settings "
        "WHERE database = currentDatabase() AND table = 't' AND source = 'config' ORDER BY name"
    ).split() == [
        "max_suspicious_broken_parts",
        "merge_max_block_size",
        "vertical_merge_algorithm_min_columns_to_activate",
        "vertical_merge_algorithm_min_rows_to_activate",
    ]

    node.query("DROP TABLE t SYNC")


def test_config_assignment_is_reported_for_replicated(started_cluster):
    # The replicated family reads an additional config section and has its own cached baseline, so
    # it is attributed separately and needs its own coverage.
    node.query("DROP TABLE IF EXISTS tr SYNC")
    node.query(
        "CREATE TABLE tr (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tr', '1') ORDER BY x"
    )

    assert source_of("merge_max_block_size", "tr") == "config"
    assert source_of("merge_max_block_size_bytes", "tr") == "default"

    # A setting the `<replicated_merge_tree>` section alone assigns: the replicated baseline must carry it,
    # and the plain one must not, which is what keeps the two baselines and their sources apart.
    assert source_of("max_replicated_merges_in_queue", "tr") == "config"
    assert (
        node.query(
            "SELECT value FROM system.table_settings WHERE database = currentDatabase() "
            "AND table = 'tr' AND name = 'max_replicated_merges_in_queue'"
        ).strip()
        == "77"
    )

    node.query("DROP TABLE IF EXISTS t_plain SYNC")
    node.query("CREATE TABLE t_plain (x UInt64) ENGINE = MergeTree ORDER BY x")
    assert source_of("max_replicated_merges_in_queue", "t_plain") == "default"
    node.query("DROP TABLE t_plain SYNC")

    node.query("DROP TABLE tr SYNC")

