#!/usr/bin/env python3


import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain_with_retry, wait_condition

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_old_dirs_cleanup(start_cluster):
    node1.query("DROP TABLE IF EXISTS test_table SYNC")
    node1.query(
        """
        CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_table', 'node1')
        PARTITION BY date ORDER BY id
        SETTINGS cleanup_delay_period=3600, max_cleanup_delay_period=3600
        """
    )

    node1.query("INSERT INTO test_table VALUES (toDate('2020-01-01'), 1, 10)")
    assert node1.query("SELECT count() FROM test_table") == "1\n"

    data_path = node1.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE database='default' AND name='test_table'"
    ).strip()

    node1.stop_clickhouse()

    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"mv {data_path}/20200101_0_0_0 {data_path}/delete_tmp_20200101_0_0_0",
        ],
        privileged=True,
    )

    node1.start_clickhouse()

    assert_logs_contain_with_retry(node1, "Removing temporary directory .*delete_tmp_20200101_0_0_0")

    assert_logs_contain_with_retry(node1, "Created empty part 20200101_0_0_0 instead of lost part")
    # Replaced empty part
    result = node1.exec_in_container(
        ["bash", "-c", f"ls {data_path}/"],
        privileged=True,
    )
    assert "20200101_0_0_0" in result
    assert node1.query("SELECT count() FROM test_table") == "0\n"

    node1.query("DROP TABLE test_table SYNC")


def test_readonly_toggle_preserves_stopped_cleanup(start_cluster):
    node1.query(
        """
        CREATE TABLE readonly_toggle_cleanup (n UInt64) ENGINE = MergeTree ORDER BY n
        SETTINGS disk = 'default', temporary_directories_lifetime = 0,
            merge_tree_clear_old_temporary_directories_interval_seconds = 1,
            cleanup_delay_period = 1, max_cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0
        """
    )
    try:
        node1.query("SYSTEM STOP CLEANUP readonly_toggle_cleanup")
        table_path = node1.query(
            "SELECT data_paths[1] FROM system.tables "
            "WHERE database = currentDatabase() AND name = 'readonly_toggle_cleanup'"
        ).strip()
        cleanup_dir = f"{table_path}/tmp_readonly_toggle_cleanup"
        node1.exec_in_container(["mkdir", cleanup_dir])

        node1.query(
            """
            ALTER TABLE readonly_toggle_cleanup MODIFY SETTING table_readonly = 1;
            ALTER TABLE readonly_toggle_cleanup MODIFY SETTING table_readonly = 0;
            """
        )
        assert node1.path_exists(cleanup_dir), "Cleanup must remain stopped"

        node1.query("SYSTEM START CLEANUP readonly_toggle_cleanup")
        wait_condition(
            lambda: node1.path_exists(cleanup_dir),
            lambda exists: not exists,
            max_attempts=60,
            delay=1,
        )
    finally:
        node1.query("DROP TABLE readonly_toggle_cleanup SYNC")


def test_writable_workers_after_cleanup_error(start_cluster):
    node1.query(
        "CREATE TABLE readonly_cleanup_error (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = 'default', table_readonly = 1, "
        "cleanup_delay_period = 3600, max_cleanup_delay_period = 3600"
    )
    data_path = node1.query(
        "SELECT data_paths[1] FROM system.tables "
        "WHERE database = currentDatabase() AND name = 'readonly_cleanup_error'"
    ).strip()
    bad_dir = data_path + "tmp_merge_cleanup_error"
    try:
        node1.exec_in_container(["mkdir", bad_dir])
        node1.exec_in_container(["ln", "-s", "loop", bad_dir + "/loop"])
        node1.exec_in_container(["touch", "-d", "2000-01-01 UTC", bad_dir])
        error = node1.query_and_get_error(
            "ALTER TABLE readonly_cleanup_error MODIFY SETTING table_readonly = 0"
        )
        assert "Too many levels of symbolic links" in error
        node1.exec_in_container(["rm", bad_dir + "/loop"])

        node1.query("INSERT INTO readonly_cleanup_error VALUES (0)")
        node1.query(
            "ALTER TABLE readonly_cleanup_error UPDATE x = 1 WHERE 1 SETTINGS mutations_sync = 0"
        )
        assert_eq_with_retry(node1, "SELECT x FROM readonly_cleanup_error", "1")
    finally:
        node1.exec_in_container(["rm", "-rf", bad_dir])
        node1.query("DROP TABLE readonly_cleanup_error SYNC")
