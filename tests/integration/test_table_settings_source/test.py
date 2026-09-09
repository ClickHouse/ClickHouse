import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `system.table_settings.source` says where a setting's effective value came from. The server config
# is one of the answers it can give, and it cannot be tested from `tests/queries/0_stateless`: that
# suite shares a server whose config a test cannot set, so an assertion there would only pin down
# whichever configuration the suite happens to run with.
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

    # The setting the config assigned the value it already had. Reported as `config` because the
    # assignment is recorded when the baseline is built; comparing values could never see it.
    assert node.query(
        "SELECT value = default FROM system.table_settings "
        "WHERE database = currentDatabase() AND table = 't' AND name = 'merge_max_block_size'"
    ).strip() == "1"
    assert source_of("merge_max_block_size") == "config"

    # The setting the config assigned a different value. This one a comparison of values would also
    # have found, so it guards the case that already worked.
    assert source_of("max_suspicious_broken_parts") == "config"

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

    node.query("DROP TABLE tr SYNC")


def test_definition_wins_over_config(started_cluster):
    # The table's own SETTINGS clause is applied last, so it outranks the config section.
    node.query("DROP TABLE IF EXISTS td SYNC")
    node.query(
        "CREATE TABLE td (x UInt64) ENGINE = MergeTree ORDER BY x "
        "SETTINGS merge_max_block_size = 8192"
    )

    assert source_of("merge_max_block_size", "td") == "definition"

    node.query("DROP TABLE td SYNC")
