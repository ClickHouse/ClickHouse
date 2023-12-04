import logging
import time
import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock, start_mock_servers
from helpers.utility import generate_values, replace_config, SafeThread

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "config.d/storage_conf.xml",
            ],
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("node_name", ["node"])
def test_parallel_cache_loading_on_startup(cluster, node_name):
    node = cluster.instances[node_name]
    node.query(
        """
        DROP TABLE IF EXISTS test SYNC;

        CREATE TABLE test (key UInt32, value String)
        Engine=MergeTree()
        ORDER BY value
        SETTINGS disk = disk(
            type = cache,
            path = 'paralel_loading_test',
            disk = 'hdd_blob',
            max_file_segment_size = '1Ki',
            boundary_alignemt = '1Ki',
            max_size = '1Gi',
            max_elements = 10000000,
            load_metadata_threads = 30);

        SYSTEM DROP FILESYSTEM CACHE;
        INSERT INTO test SELECT * FROM generateRandom('a Int32, b String') LIMIT 1000000;
        SELECT * FROM test FORMAT Null;
        """
    )
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0
    assert int(node.query("SELECT max(size) FROM system.filesystem_cache")) == 1024
    count = int(node.query("SELECT count() FROM test"))

    cache_count = int(
        node.query("SELECT count() FROM system.filesystem_cache WHERE size > 0")
    )
    cache_state = node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size"
    )

    node.restart_clickhouse()

    assert cache_count == int(node.query("SELECT count() FROM system.filesystem_cache"))
    assert cache_state == node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache ORDER BY key, file_segment_range_begin, size"
    )

    assert node.contains_in_log("Loading filesystem cache with 30 threads")
    assert int(node.query("SELECT count() FROM system.filesystem_cache")) > 0
    assert int(node.query("SELECT max(size) FROM system.filesystem_cache")) == 1024
    assert (
        int(
            node.query(
                "SELECT value FROM system.events WHERE event = 'FilesystemCacheLoadMetadataMicroseconds'"
            )
        )
        > 0
    )
    node.query("SELECT * FROM test FORMAT Null")
    assert count == int(node.query("SELECT count() FROM test"))
