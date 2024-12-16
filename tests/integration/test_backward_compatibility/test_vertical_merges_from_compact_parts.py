import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_old = cluster.add_instance(
    "node1",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
    with_zookeeper=True,
)
node_new = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/no_compress_marks.xml",
        "configs/no_allow_vertical_merges_from_compact_to_wide_parts.xml",
    ],
    with_zookeeper=True,
    stay_alive=True,
    use_old_analyzer=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_vertical_merges_from_compact_parts(start_cluster):
    for i, node in enumerate([node_old, node_new]):
        node.query(
            """
            CREATE TABLE t_vertical_merges (id UInt64, v1 UInt64, v2 UInt64)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/t_vertical_merges', '{}')
            ORDER BY id
            SETTINGS
                index_granularity = 50,
                vertical_merge_algorithm_min_rows_to_activate = 1,
                vertical_merge_algorithm_min_columns_to_activate = 1,
                min_bytes_for_wide_part = 0,
                min_rows_for_wide_part = 100
        """.format(
                i
            )
        )

    node_new.query(
        "INSERT INTO t_vertical_merges SELECT number, number, number FROM numbers(60)"
    )
    node_new.query(
        "INSERT INTO t_vertical_merges SELECT number * 2, number, number FROM numbers(60)"
    )
    node_new.query("OPTIMIZE TABLE t_vertical_merges FINAL")
    node_old.query("SYSTEM SYNC REPLICA t_vertical_merges")

    check_query = """
        SELECT merge_algorithm, part_type FROM system.part_log
        WHERE event_type = 'MergeParts' AND table = 't_vertical_merges' AND part_name = '{}';
    """

    node_new.query("SYSTEM FLUSH LOGS")
    node_old.query("SYSTEM FLUSH LOGS")

    assert node_new.query(check_query.format("all_0_1_1")) == "Horizontal\tWide\n"
    assert node_old.query(check_query.format("all_0_1_1")) == "Horizontal\tWide\n"

    node_new.query(
        "ALTER TABLE t_vertical_merges MODIFY SETTING allow_vertical_merges_from_compact_to_wide_parts = 1"
    )

    node_new.query(
        "INSERT INTO t_vertical_merges SELECT number * 3, number, number FROM numbers(60)"
    )
    node_new.query("OPTIMIZE TABLE t_vertical_merges FINAL")
    node_old.query("SYSTEM SYNC REPLICA t_vertical_merges")

    node_new.query("SYSTEM FLUSH LOGS")
    node_old.query("SYSTEM FLUSH LOGS")

    assert node_old.contains_in_log(
        "CHECKSUM_DOESNT_MATCH"
    ) or node_new.contains_in_log("CHECKSUM_DOESNT_MATCH")

    assert node_new.query(check_query.format("all_0_2_2")) == "Vertical\tWide\n"
    assert node_old.query(check_query.format("all_0_2_2")) == "Horizontal\tWide\n"

    node_old.restart_with_latest_version()
    node_new.restart_clickhouse()

    node_old.query(
        "ALTER TABLE t_vertical_merges MODIFY SETTING allow_vertical_merges_from_compact_to_wide_parts = 1"
    )

    node_new.query(
        "INSERT INTO t_vertical_merges SELECT number * 4, number, number FROM numbers(60)"
    )
    node_new.query("OPTIMIZE TABLE t_vertical_merges FINAL")
    node_old.query("SYSTEM SYNC REPLICA t_vertical_merges")

    node_new.query("SYSTEM FLUSH LOGS")
    node_old.query("SYSTEM FLUSH LOGS")

    assert not (
        # Now the old node is restarted as a new, and its config allows compressed indices, and it merged the data into compressed indices,
        # that's why the error about different number of compressed files is expected and ok.
        (
            node_old.contains_in_log("CHECKSUM_DOESNT_MATCH")
            and not node_old.contains_in_log("Different number of files")
        )
        or (
            node_new.contains_in_log("CHECKSUM_DOESNT_MATCH")
            and not node_new.contains_in_log("Different number of files")
        )
    )

    assert node_new.query(check_query.format("all_0_3_3")) == "Vertical\tWide\n"
    assert node_old.query(check_query.format("all_0_3_3")) == "Vertical\tWide\n"
