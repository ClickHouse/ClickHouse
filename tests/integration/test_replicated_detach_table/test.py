# Tag no-fasttest: requires S3

import logging
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

replica1 = cluster.add_instance(
    "replica1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica1"},
)
replica2 = cluster.add_instance(
    "replica2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml"],
    macros={"replica": "replica2"},
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test_replicated_detach_table(start_cluster):
    replica1.query(
        f"CREATE DATABASE IF NOT EXISTS repl_db ON CLUSTER test_cluster ENGINE=Replicated('/clickhouse/tables/test_replicated_table', shard1, '{{replica}}');"
    )
    replica1.query_with_retry(
        f"""
        CREATE TABLE repl_db.test_replicated_table
        (
            number UInt64
        ) 
        ENGINE=ReplicatedMergeTree
        ORDER BY number
        """
    )

    replica1.query(
        "INSERT INTO repl_db.test_replicated_table SELECT number FROM system.numbers LIMIT 6;"
    )
    replica1.query("SYSTEM SYNC REPLICA repl_db.test_replicated_table;", timeout=20)
    replica1.query("DETACH TABLE repl_db.test_replicated_table PERMANENTLY;")
