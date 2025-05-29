import csv
import logging
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers, start_s3_mock


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=["configs/storage_conf.xml"],
            user_configs=[],
            with_minio=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2",
            main_configs=["configs/storage_conf_2.xml"],
            user_configs=[],
            with_minio=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def test_simple(started_cluster):
    node1 = started_cluster.instances["node1"]
    node2 = started_cluster.instances["node2"]

    node1.query("SYSTEM STOP MERGES")
    node1.query("""
        CREATE TABLE base (a Int32) ENGINE=MergeTree() ORDER BY tuple()
        SETTINGS disk = 's3_plain'
    """)

    node1.query("INSERT INTO base SELECT number FROM numbers(10)")

    uuid = node1.query("SELECT uuid FROM system.tables WHERE name = 'base'").strip()

    # We have to restart the server because unforunately
    # PlainRewritable metadata is only loaded on server startup and not updated afterwards
    # so without restart we would not see files created with the above insert.
    # Maybe add a flag to PlainRewritable metadata constructor "update_metadata_after_start"
    # and start a background thread to refresh metadata in the background in case it changes?
    node2.restart_clickhouse()

    node2.query(
        f"""
        CREATE TABLE overlay UUID '{uuid}' (a Int32)
        ENGINE=MergeTree() ORDER BY tuple()
        SETTINGS disk = 'overlay'
    """)
    assert 10 == int(node2.query("SELECT count() FROM overlay"))

    # TODO: make this test more complex to cover all disk operations.
    # E.g. insert more data into overlay disk, check that merges are tested, backup, etc.

    # TODO: Add a test with ReplicatedMergeTree.

    # TODO: Support creating DiskOverlay via AST
    # e.g. CREATE TABLE test ... SETTINGS disk = disk(type = overlay, diff_disk='', ...);
    # Currently getDiskConfigurationFromAST() does not support dotted keys,
    # but we need one to specify, for example, forward_metadata.metadata_type.
    # See src/Disks/getDiskConfigurationFromAST.cpp.

    # TODO:  Check non-local metadata type for forward metadata and tracked metadata.
