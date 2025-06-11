# Tag no-fasttest: requires S3

import random
import string

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


def generate_random_string(length=8):
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


@pytest.fixture(scope="function")
def exclusive_database_name():
    return "repl_db_" + generate_random_string()


def test_replicated_detach_table(start_cluster, exclusive_database_name):
    replica1.query(
        f"CREATE DATABASE IF NOT EXISTS {exclusive_database_name} ON CLUSTER test_cluster ENGINE=Replicated('/clickhouse/tables/{exclusive_database_name}/test_replicated_table', shard1, '{{replica}}');"
    )

    replica1.query_with_retry(
        f"""
        CREATE TABLE {exclusive_database_name}.test_replicated_table
        (
            number UInt64
        ) 
        ENGINE=ReplicatedMergeTree
        ORDER BY number
        """
    )
    replica1.query(
        f"INSERT INTO {exclusive_database_name}.test_replicated_table SELECT number FROM system.numbers LIMIT 6;"
    )
    replica1.query(
        f"SYSTEM SYNC REPLICA {exclusive_database_name}.test_replicated_table;",
        timeout=20,
    )
    replica1.query(
        f"DETACH TABLE {exclusive_database_name}.test_replicated_table PERMANENTLY;"
    )

    replica1.query(
        f"DROP DATABASE {exclusive_database_name} ON CLUSTER test_cluster SYNC"
    )
