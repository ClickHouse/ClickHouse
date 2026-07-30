import pytest

from helpers.cluster import ClickHouseCluster

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        # Until 25.12 index filenames weren't escaped
        cluster.add_instance(
            "old_node",
            image="clickhouse/clickhouse-server",
            tag="25.12.3.21",
            with_installed_binary=True,
            stay_alive=True,
        )
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_index_filename_upgrade(started_cluster):
    node = started_cluster.instances["old_node"]

    node.query("DROP TABLE IF EXISTS test_index_filename;")
    node.query(
        f"""
        CREATE TABLE test_index_filename (
            column UInt8,
            INDEX `minmax_index_ESPAÑA` `column` TYPE set(0) GRANULARITY 1
        )
        ENGINE = MergeTree ORDER BY tuple()
        SETTINGS add_minmax_index_for_numeric_columns=0, min_bytes_for_wide_part=1000;
        """
    )
    node.query("INSERT INTO test_index_filename SELECT number FROM numbers(1); -- Compact / packed")
    node.query("INSERT INTO test_index_filename SELECT number FROM numbers(10000); -- Wide")
    node.query("SELECT count() FROM test_index_filename WHERE `column` = 24 SETTINGS use_query_condition_cache=0, use_skip_indexes_on_data_read=0;")
    node.wait_for_log_line("Index `minmax_index_ESPAÑA` has dropped 1")

    # When restarting with the latest version, the index filename is expected to be escaped so loading it will fail
    node.restart_with_latest_version()

    node.query("SELECT count() FROM test_index_filename WHERE `column` = 24 SETTINGS use_query_condition_cache=0, use_skip_indexes_on_data_read=0;")
    node.wait_for_log_line("File for index `minmax_index_ESPAÑA` does not exist")
    node.wait_for_log_line("Index `minmax_index_ESPAÑA` has dropped 0")

    node.query("ALTER TABLE test_index_filename MODIFY SETTING escape_index_filenames=0;")

    # And now the index should work
    node.query("SELECT count() FROM test_index_filename WHERE `column` = 24 SETTINGS use_query_condition_cache=0, use_skip_indexes_on_data_read=0;")
    node.wait_for_log_line("Index `minmax_index_ESPAÑA` has dropped 1")

    node.query("DROP TABLE test_index_filename;")

    # Leave the node in the original state: Just to avoid flakiness when running it multiple times
    node.restart_with_original_version()
