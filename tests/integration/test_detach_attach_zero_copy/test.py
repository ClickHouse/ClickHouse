import time
from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance(
    "ch1",
    main_configs=[
        "config.xml",
    ],
    macros={"replica": "node1"},
    with_zookeeper=True,
    with_minio=True,
)
ch2 = cluster.add_instance(
    "ch2",
    main_configs=[
        "config.xml",
    ],
    macros={"replica": "node2"},
    with_zookeeper=True,
    with_minio=True,
)

DATABASE_NAME = "detach_attach_zero_copy"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(node, query):
    return node.query(database=DATABASE_NAME, sql=query)


# kazoo.delete may throw NotEmptyError on concurrent modifications of the path
def zk_rmr_with_retries(zk, path):
    for i in range(1, 10):
        try:
            zk.delete(path, recursive=True)
            return
        except Exception as ex:
            print(ex)
            time.sleep(0.5)
    assert False


def grep_log(node, s):
    return node.exec_in_container(
        ["bash", "-c", f"grep '{s}' /var/log/clickhouse-server/clickhouse-server.log"]
    )


def test_blobs_deleted_on_part_reattach(started_cluster):
    zk = cluster.get_kazoo_client("zoo1")

    ch1.query(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} ON CLUSTER cluster")

    q(
        ch1,
        "CREATE TABLE test ON CLUSTER cluster ( id UInt32 ) ENGINE ReplicatedMergeTree('/clickhouse/tables/test', '{replica}') ORDER BY id;",
    )

    q(ch1, "INSERT INTO test SELECT number FROM numbers(100);")
    q(ch2, "SYSTEM SYNC REPLICA test")

    assert "all_0_0_0\n" == q(ch1, "SELECT name FROM system.parts WHERE table='test'")

    q(ch1, "DETACH TABLE test")
    q(ch2, "DETACH TABLE test")
    q(ch1, "SYSTEM DROP REPLICA 'node1' FROM ZKPATH '/clickhouse/tables/test'")
    q(ch2, "SYSTEM DROP REPLICA 'node2' FROM ZKPATH '/clickhouse/tables/test'")
    # Zero copy locks are not dropping when replica is dropping, part name collision is possible
    zk_rmr_with_retries(zk, "/clickhouse/zero_copy/zero_copy_s3")
    q(ch1, "ATTACH TABLE test")
    q(ch2, "ATTACH TABLE test")

    q(ch1, "SYSTEM RESTORE REPLICA test")

    assert "all_0_0_0\n" == q(ch1, "SELECT name FROM system.parts WHERE table='test'")
    assert "all_0_0_0\n" == q(ch2, "SELECT name FROM system.parts WHERE table='test'")

    q(ch2, "SYSTEM RESTORE REPLICA test")

    # When replica on ch2 is restored, metadata_version.txt file in part has reference on deleted blob.
    # If read exception from file is not ignored, part will be detached as broken and fetched from other replica.
    assert "" == q(ch2, "SELECT name FROM system.detached_parts WHERE table='test'")

    assert grep_log(ch2, "Failed to read metadata version") == "1213"

    q(ch1, f"DROP TABLE test ON CLUSTER cluster")
