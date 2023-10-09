import time
import pytest
import os
import logging

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_foundationdb=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query(
            """
        CREATE TABLE test_table(date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/shard1/test/test_table', '1')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
    """
        )

        yield cluster
    finally:
        cluster.shutdown()


def test_reload_fdbkeeper(start_cluster):
    def replace_fdbkeeper_cluster(new_fdb_cluster):
        logging.debug(f"Use new fdb cluster: {new_fdb_cluster}")
        node.replace_config("/tmp/fdb.cluster", new_fdb_cluster)
        node.replace_in_config(
            "/etc/clickhouse-server/conf.d/fdb_config.xml",
            "\\/etc\\/foundationdb\\/fdb.cluster",
            "\\/tmp\\/fdb.cluster",
        )
        node.query("SYSTEM RELOAD CONFIG")

    node.query(
        "INSERT INTO test_table(date, id) select today(), number FROM numbers(1000)"
    )

    ## stop fdb, table will be readonly
    cluster.stop_fdb()
    node.query("SELECT COUNT() FROM test_table")
    with pytest.raises(QueryRuntimeException):
        node.query(
            "SELECT COUNT() FROM test_table",
            settings={"select_sequential_consistency": 1},
        )

    ## switch to fdb2, server will be normal
    cluster.switch_to_fdb2()
    replace_fdbkeeper_cluster(cluster.get_fdb2_cluster())
    assert_eq_with_retry(
        node, "SELECT COUNT() FROM test_table", "1000", retry_count=120, sleep_time=0.5
    )
