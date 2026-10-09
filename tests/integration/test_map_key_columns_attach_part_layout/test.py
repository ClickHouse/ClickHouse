# A detached part written with another Map layout cannot be attached, even when
# `serialization_info_version = 'basic'` does not record the layout in
# `serialization.json`. The test moves part directories between tables on disk,
# so it lives here rather than in stateless tests.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def table_data_path(table):
    return node.query(
        f"SELECT data_paths[1] FROM system.tables WHERE database = 'default' AND table = '{table}'"
    ).strip()


@pytest.mark.parametrize("min_bytes_for_wide_part", [0, 10000000000], ids=["wide", "compact"])
def test_attach_part_with_other_map_layout(started_cluster, min_bytes_for_wide_part):
    settings = f"min_bytes_for_wide_part = {min_bytes_for_wide_part}, serialization_info_version = 'basic', propagate_types_serialization_versions_to_nested_types = 1"
    columns = "id UInt64, m Map(String, UInt64), am Array(Map(String, UInt64))"
    node.query("DROP TABLE IF EXISTS t_layout_basic SYNC")
    node.query("DROP TABLE IF EXISTS t_layout_keys SYNC")
    node.query(f"CREATE TABLE t_layout_basic ({columns}) ENGINE = MergeTree ORDER BY id SETTINGS {settings}")
    node.query(
        f"CREATE TABLE t_layout_keys ({columns}) ENGINE = MergeTree ORDER BY id "
        f"SETTINGS {settings}, map_serialization_version = 'with_key_columns'"
    )
    node.query("INSERT INTO t_layout_basic VALUES (1, {'a': 1}, [{'a': 1}])")
    node.query("INSERT INTO t_layout_keys VALUES (2, {'b': 2}, [{'b': 2}])")
    node.query("ALTER TABLE t_layout_basic DETACH PART 'all_1_1_0'")
    node.query("ALTER TABLE t_layout_keys DETACH PART 'all_1_1_0'")

    path_basic = table_data_path("t_layout_basic")
    path_keys = table_data_path("t_layout_keys")
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {path_basic}detached/all_1_1_0 {path_keys}detached/all_9_9_0 && "
            f"mv {path_keys}detached/all_1_1_0 {path_basic}detached/all_9_9_0",
        ],
        privileged=True,
        user="root",
    )

    for table in ["t_layout_keys", "t_layout_basic"]:
        error = node.query_and_get_error(f"ALTER TABLE {table} ATTACH PART 'all_9_9_0'")
        assert "INCOMPATIBLE_COLUMNS" in error, error
        assert node.query(f"SELECT count() FROM {table}") == "0\n"

    node.query("DROP TABLE t_layout_basic SYNC")
    node.query("DROP TABLE t_layout_keys SYNC")
