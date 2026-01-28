import fnmatch
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/config_without_standard_part_log.xml"]
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/config_with_standard_part_log.xml"]
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/config_with_non_standard_part_log.xml"]
)
node4 = cluster.add_instance(
    "node4", main_configs=["configs/config_disk_name_test.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_config_without_part_log(start_cluster):
    table_name = f"test_table_{uuid.uuid4().hex}"

    resp = node1.query_and_get_error("SELECT * FROM system.part_log")
    assert fnmatch.fnmatch(resp, "*DB::Exception:*system.part_log*UNKNOWN_TABLE*"), resp

    node1.query(
        f"CREATE TABLE {table_name}(word String, value UInt64) ENGINE=MergeTree() ORDER BY value"
    )
    resp = node1.query_and_get_error("SELECT * FROM system.part_log")
    assert fnmatch.fnmatch(resp, "*DB::Exception:*system.part_log*UNKNOWN_TABLE*"), resp

    node1.query(f"INSERT INTO {table_name} VALUES ('name', 1)")
    node1.query("SYSTEM FLUSH LOGS part_log")

    resp = node1.query_and_get_error("SELECT * FROM system.part_log")
    assert fnmatch.fnmatch(resp, "*DB::Exception:*system.part_log*UNKNOWN_TABLE*"), resp


# Note: if part_log is defined, we cannot say when the table will be created - because of metric_log, trace_log, text_log, query_log...


def test_config_with_standard_part_log(start_cluster):
    table_name = f"test_table_{uuid.uuid4().hex}"

    node2.query(f"CREATE TABLE {table_name}(word String, value UInt64) ENGINE=MergeTree() Order by value")
    node2.query(f"INSERT INTO {table_name} VALUES ('name', 1)")
    node2.query("SYSTEM FLUSH LOGS part_log")
    assert node2.query(f"SELECT * FROM system.part_log WHERE database = 'default' AND table = '{table_name}'") != ""


def test_part_log_contains_partition(start_cluster):
    table_name = f"test_partition_table_{uuid.uuid4().hex}"

    node2.query(f"CREATE TABLE {table_name} (date Date, word String, value UInt64) ENGINE=MergeTree() PARTITION BY toYYYYMM(date) Order by value")
    node2.query(
        f"INSERT INTO {table_name} VALUES "
        + "('2023-06-20', 'a', 10), ('2023-06-21', 'b', 11),"
        + "('2023-05-20', 'cc', 14),('2023-05-21', 'd1', 15);"
    )
    node2.query("SYSTEM FLUSH LOGS part_log")
    resp = node2.query(
        f"SELECT partition from system.part_log where table = '{table_name}'"
    )
    assert resp == "202306\n202305\n"


def test_system_log_tables_in_part_log(start_cluster):
    table_name = f"tmp_table_{uuid.uuid4().hex}"

    node2.query(f"CREATE TABLE {table_name}(key Int) ENGINE=MergeTree() ORDER BY ()")
    node2.query(f"INSERT INTO {table_name} VALUES (1)")
    node2.query("SYSTEM FLUSH LOGS part_log")
    node2.query("OPTIMIZE TABLE system.part_log FINAL")
    node2.query("SYSTEM FLUSH LOGS part_log")
    assert node2.query("SELECT DISTINCT event_type FROM system.part_log WHERE database = 'system' AND table = 'part_log' ORDER BY ALL") == "NewPart\nMergeParts\nMergePartsStart\n"


def test_config_with_non_standard_part_log(start_cluster):
    table_name = f"test_table_{uuid.uuid4().hex}"

    node3.query(f"CREATE TABLE {table_name}(word String, value UInt64) ENGINE=MergeTree() Order by value")
    node3.query(f"INSERT INTO {table_name} VALUES ('name', 1)")
    node3.query("SYSTEM FLUSH LOGS part_log")
    assert node3.query(f"SELECT * FROM system.own_part_log WHERE table = '{table_name}'") != ""


def test_config_disk_name_test(start_cluster):
    table_name1 = f"test_table1_{uuid.uuid4().hex}"
    table_name2 = f"test_table2_{uuid.uuid4().hex}"

    node4.query(f"CREATE TABLE {table_name1}(word String, value UInt64) ENGINE = MergeTree() ORDER BY word SETTINGS storage_policy = 'test1'")
    node4.query(f"INSERT INTO {table_name1}(*) VALUES ('test1', 2)")
    node4.query(f"CREATE TABLE {table_name2}(word String, value UInt64) ENGINE = MergeTree() ORDER BY word SETTINGS storage_policy = 'test2'")
    node4.query(f"INSERT INTO {table_name2}(*) VALUES ('test2', 3)")
    node4.query("SYSTEM FLUSH LOGS part_log")
    assert (
        node4.query(f"SELECT DISTINCT disk_name FROM system.part_log WHERE database = 'default' AND table IN ('{table_name1}', '{table_name2}') ORDER by disk_name")
        == "test1\ntest2\n"
    )
