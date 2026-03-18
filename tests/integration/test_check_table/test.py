import concurrent

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def remove_checksums_on_disk(node, database, table, part_name):
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE database = '{database}' AND table = '{table}' AND name = '{part_name}'"
    ).strip()
    node.exec_in_container(
        ["bash", "-c", "rm -r {p}/checksums.txt".format(p=part_path)], privileged=True
    )


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(
            table, part_name
        )
    ).strip()
    if not part_path:
        raise Exception("Part " + part_name + "doesn't exist")
    node.exec_in_container(
        ["bash", "-c", "rm -r {p}/*".format(p=part_path)], privileged=True
    )


@pytest.mark.parametrize("merge_tree_settings", [""])
def test_check_normal_table_corruption(started_cluster, merge_tree_settings):
    node1.query("DROP TABLE IF EXISTS non_replicated_mt")

    node1.query(
        f"""
        CREATE TABLE non_replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id
        {merge_tree_settings};
    """
    )

    node1.query(
        "INSERT INTO non_replicated_mt VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
    )
    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt PARTITION 201902",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201902_1_1_0\t1\t\n"
    )

    remove_checksums_on_disk(node1, "default", "non_replicated_mt", "201902_1_1_0")

    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        ).strip()
        == "201902_1_1_0\t1\tChecksums recounted and written to disk."
    )

    assert node1.query("SELECT COUNT() FROM non_replicated_mt") == "2\n"

    remove_checksums_on_disk(node1, "default", "non_replicated_mt", "201902_1_1_0")

    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt PARTITION 201902",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        ).strip()
        == "201902_1_1_0\t1\tChecksums recounted and written to disk."
    )

    assert node1.query("SELECT COUNT() FROM non_replicated_mt") == "2\n"

    corrupt_part_data_on_disk(
        node1, "non_replicated_mt", "201902_1_1_0", database="default"
    )

    assert node1.query(
        "CHECK TABLE non_replicated_mt",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ).strip().split("\t")[0:2] == ["201902_1_1_0", "0"]

    assert node1.query(
        "CHECK TABLE non_replicated_mt",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ).strip().split("\t")[0:2] == ["201902_1_1_0", "0"]

    node1.query(
        "INSERT INTO non_replicated_mt VALUES (toDate('2019-01-01'), 1, 10), (toDate('2019-01-01'), 2, 12)"
    )

    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt PARTITION 201901",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201901_2_2_0\t1\t\n"
    )

    corrupt_part_data_on_disk(
        node1, "non_replicated_mt", "201901_2_2_0", database="default"
    )

    remove_checksums_on_disk(node1, "default", "non_replicated_mt", "201901_2_2_0")

    assert node1.query(
        "CHECK TABLE non_replicated_mt PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ).strip().split("\t")[0:2] == ["201901_2_2_0", "0"]


@pytest.mark.parametrize("merge_tree_settings, zk_path_suffix", [("", "_0")])
def test_check_replicated_table_simple(
    started_cluster, merge_tree_settings, zk_path_suffix
):
    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS replicated_mt SYNC")

        node.query(
            """
        CREATE TABLE replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt_{zk_path_suffix}', '{replica}')
        PARTITION BY toYYYYMM(date) ORDER BY id
        {merge_tree_settings}
            """.format(
                replica=node.name,
                zk_path_suffix=zk_path_suffix,
                merge_tree_settings=merge_tree_settings,
            )
        )

    node1.query(
        "INSERT INTO replicated_mt VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
    )
    node2.query("SYSTEM SYNC REPLICA replicated_mt")

    assert node1.query("SELECT count() from replicated_mt") == "2\n"
    assert node2.query("SELECT count() from replicated_mt") == "2\n"

    assert (
        node1.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201902_0_0_0\t1\t\n"
    )
    assert (
        node2.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201902_0_0_0\t1\t\n"
    )

    node2.query(
        "INSERT INTO replicated_mt VALUES (toDate('2019-01-02'), 3, 10), (toDate('2019-01-02'), 4, 12)"
    )
    node1.query("SYSTEM SYNC REPLICA replicated_mt")
    assert node1.query("SELECT count() from replicated_mt") == "4\n"
    assert node2.query("SELECT count() from replicated_mt") == "4\n"

    assert (
        node1.query(
            "CHECK TABLE replicated_mt PARTITION 201901",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201901_0_0_0\t1\t\n"
    )
    assert (
        node2.query(
            "CHECK TABLE replicated_mt PARTITION 201901",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201901_0_0_0\t1\t\n"
    )

    assert sorted(
        node2.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0},
        ).split("\n")
    ) == ["", "201901_0_0_0\t1\t", "201902_0_0_0\t1\t"]

    with pytest.raises(QueryRuntimeException) as exc:
        node2.query(
            "CHECK TABLE replicated_mt PART '201801_0_0_0'",
            settings={"check_query_single_value_result": 0},
        )
    assert "NO_SUCH_DATA_PART" in str(exc.value)

    assert (
        node2.query(
            "CHECK TABLE replicated_mt PART '201902_0_0_0'",
            settings={"check_query_single_value_result": 0},
        )
        == "201902_0_0_0\t1\t\n"
    )


@pytest.mark.parametrize(
    "merge_tree_settings, zk_path_suffix, part_file_ext",
    [
        (
            "",
            "_0",
            ".bin",
        )
    ],
)
def test_check_replicated_table_corruption(
    started_cluster, merge_tree_settings, zk_path_suffix, part_file_ext
):
    for node in [node1, node2]:
        node.query_with_retry("DROP TABLE IF EXISTS replicated_mt_1 SYNC")

        node.query_with_retry(
            """
        CREATE TABLE replicated_mt_1(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt_1_{zk_path_suffix}', '{replica}')
        PARTITION BY toYYYYMM(date) ORDER BY id
        {merge_tree_settings}
            """.format(
                replica=node.name,
                merge_tree_settings=merge_tree_settings,
                zk_path_suffix=zk_path_suffix,
            )
        )

    node1.query(
        "INSERT INTO replicated_mt_1 VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
    )
    node1.query(
        "INSERT INTO replicated_mt_1 VALUES (toDate('2019-01-02'), 3, 10), (toDate('2019-01-02'), 4, 12)"
    )
    node2.query("SYSTEM SYNC REPLICA replicated_mt_1")

    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"
    assert node2.query("SELECT count() from replicated_mt_1") == "4\n"

    part_name = node1.query_with_retry(
        "SELECT name from system.parts where table = 'replicated_mt_1' and partition_id = '201901' and active = 1"
    ).strip()

    corrupt_part_data_on_disk(
        node1, "replicated_mt_1", part_name, part_file_ext, database="default"
    )

    assert node1.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{p}\t0\tPart {p} looks broken. Removing it and will try to fetch.\n".format(
        p=part_name
    )

    node1.query_with_retry("SYSTEM SYNC REPLICA replicated_mt_1")
    assert node1.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{}\t1\t\n".format(part_name)
    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"

    remove_part_from_disk(node2, "replicated_mt_1", part_name)
    assert node2.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{p}\t0\tPart {p} looks broken. Removing it and will try to fetch.\n".format(
        p=part_name
    )

    node1.query("SYSTEM SYNC REPLICA replicated_mt_1")
    assert node1.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{}\t1\t\n".format(part_name)
    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"


def test_check_all_tables(started_cluster):
    def _create_table(database, table, engine=None):
        if engine is None:
            engine = "MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id SETTINGS min_bytes_for_wide_part=0"

        node1.query(
            f"CREATE TABLE {database}.{table} (date Date, id UInt32, value Int32) ENGINE = {engine}"
        )
        node1.query(
            f"INSERT INTO {database}.{table} VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
        )
        node1.query(f"SYSTEM STOP MERGES {database}.{table}")

    for database in ["db1", "db2", "db3"]:
        node1.query(f"CREATE DATABASE {database}")
        for table in ["table1", "table2"]:
            _create_table(database, table)

    remove_checksums_on_disk(node1, "db1", "table2", "201902_1_1_0")

    # These tables should not be checked
    _create_table("db1", "table_memory", "Memory()")
    _create_table("db1", "table_log", "TinyLog()")

    node1.query("SYSTEM ENABLE FAILPOINT check_table_query_delay_for_part")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []

        check_future = executor.submit(
            lambda: node1.query(
                "CHECK ALL TABLES",
                settings={"check_query_single_value_result": 0, "max_threads": 16},
            )
        )

        futures.append(executor.submit(lambda: node1.query("DROP TABLE db3.table2")))
        futures.append(executor.submit(lambda: node1.query("DROP DATABASE db2")))

        for future in concurrent.futures.as_completed(futures):
            future.result()

        check_result = check_future.result()

    # Do not check databases created in other tests
    check_result = [
        res.split("\t")
        for res in check_result.split("\n")
        if res and not res.startswith("default")
    ]

    checked_tables = {f"{res[0]}.{res[1]}" for res in check_result}
    assert all(
        table in checked_tables for table in ["db1.table1", "db1.table2", "db3.table1"]
    ), checked_tables

    for check_result_row in check_result:
        assert len(check_result_row) == 5, check_result_row

        db, table, part, flag, message = check_result_row
        if db == "db1" and table == "table2" and part == "201902_1_1_0":
            assert flag == "1"
            assert message == "Checksums recounted and written to disk."
        else:
            assert flag == "1"
            assert message == ""
