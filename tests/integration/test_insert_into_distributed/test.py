import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

instance_test_reconnect = cluster.add_instance(
    "instance_test_reconnect", main_configs=["configs/remote_servers.xml"]
)
instance_test_inserts_batching = cluster.add_instance(
    "instance_test_inserts_batching",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/enable_distributed_inserts_batching.xml"],
)
remote = cluster.add_instance(
    "remote", main_configs=["configs/forbid_background_merges.xml"]
)

instance_test_inserts_local_cluster = cluster.add_instance(
    "instance_test_inserts_local_cluster", main_configs=["configs/remote_servers.xml"]
)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)

shard1 = cluster.add_instance(
    "shard1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
shard2 = cluster.add_instance(
    "shard2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        remote.query("CREATE TABLE local1 (x UInt32) ENGINE = Log")
        instance_test_reconnect.query(
            """
CREATE TABLE distributed (x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local1')
"""
        )

        remote.query(
            "CREATE TABLE local2 (d Date, x UInt32, s String) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x"
        )
        instance_test_inserts_batching.query(
            """
CREATE TABLE distributed (d Date, x UInt32) ENGINE = Distributed('test_cluster', 'default', 'local2') SETTINGS fsync_after_insert=1, fsync_directories=1
"""
        )

        instance_test_inserts_local_cluster.query(
            "CREATE TABLE local (d Date, x UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x"
        )
        instance_test_inserts_local_cluster.query(
            """
CREATE TABLE distributed_on_local (d Date, x UInt32) ENGINE = Distributed('test_local_cluster', 'default', 'local')
"""
        )

        node1.query(
            """
CREATE TABLE replicated(date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/replicated', 'node1') PARTITION BY toYYYYMM(date) ORDER BY id
"""
        )
        node2.query(
            """
CREATE TABLE replicated(date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/replicated', 'node2') PARTITION BY toYYYYMM(date) ORDER BY id
"""
        )

        node1.query(
            """
CREATE TABLE distributed (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica', 'default', 'replicated')
"""
        )

        node2.query(
            """
CREATE TABLE distributed (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica', 'default', 'replicated')
"""
        )

        shard1.query(
            """
CREATE TABLE low_cardinality (d Date, x UInt32, s LowCardinality(String)) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x"""
        )

        shard2.query(
            """
CREATE TABLE low_cardinality (d Date, x UInt32, s LowCardinality(String)) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x"""
        )

        shard1.query(
            """
CREATE TABLE low_cardinality_all (d Date, x UInt32, s LowCardinality(String)) ENGINE = Distributed('shard_with_low_cardinality', 'default', 'low_cardinality', sipHash64(s))"""
        )

        node1.query(
            """
CREATE TABLE table_function (n UInt8, s String) ENGINE = MergeTree() ORDER BY n"""
        )

        node2.query(
            """
CREATE TABLE table_function (n UInt8, s String) ENGINE = MergeTree() ORDER BY n"""
        )

        node1.query(
            """
CREATE TABLE distributed_one_replica_internal_replication (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica_internal_replication', 'default', 'single_replicated')
"""
        )

        node2.query(
            """
CREATE TABLE distributed_one_replica_internal_replication (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica_internal_replication', 'default', 'single_replicated')
"""
        )

        node1.query(
            """
CREATE TABLE distributed_one_replica_no_internal_replication (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica', 'default', 'single_replicated')
"""
        )

        node2.query(
            """
CREATE TABLE distributed_one_replica_no_internal_replication (date Date, id UInt32) ENGINE = Distributed('shard_with_local_replica', 'default', 'single_replicated')
"""
        )

        node2.query(
            """
CREATE TABLE single_replicated(date Date, id UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/single_replicated', 'node2') PARTITION BY toYYYYMM(date) ORDER BY id
"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_reconnect(started_cluster):
    instance = instance_test_reconnect

    with PartitionManager() as pm:
        # Open a connection for insertion.
        instance.query("INSERT INTO distributed VALUES (1)")
        time.sleep(1)
        assert remote.query("SELECT count(*) FROM local1").strip() == "1"

        # Now break the connection.
        pm.partition_instances(
            instance, remote, action="REJECT --reject-with tcp-reset"
        )
        instance.query("INSERT INTO distributed VALUES (2)")
        time.sleep(1)

        # Heal the partition and insert more data.
        # The connection must be reestablished and after some time all data must be inserted.
        pm.heal_all()
        time.sleep(1)
        instance.query("INSERT INTO distributed VALUES (3)")
        time.sleep(5)

        assert remote.query("SELECT count(*) FROM local1").strip() == "3"


def test_inserts_batching(started_cluster):
    instance = instance_test_inserts_batching

    with PartitionManager() as pm:
        pm.partition_instances(instance, remote)

        instance.query("INSERT INTO distributed(d, x) VALUES ('2000-01-01', 1)")
        # Sleep a bit so that this INSERT forms a batch of its own.
        time.sleep(0.1)

        instance.query("INSERT INTO distributed(x, d) VALUES (2, '2000-01-01')")

        for i in range(3, 7):
            instance.query(
                "INSERT INTO distributed(d, x) VALUES ('2000-01-01', {})".format(i)
            )

        for i in range(7, 9):
            instance.query(
                "INSERT INTO distributed(x, d) VALUES ({}, '2000-01-01')".format(i)
            )

        instance.query("INSERT INTO distributed(d, x) VALUES ('2000-01-01', 9)")

        # After ALTER the structure of the saved blocks will be different
        instance.query("ALTER TABLE distributed ADD COLUMN s String")

        for i in range(10, 13):
            instance.query(
                "INSERT INTO distributed(d, x) VALUES ('2000-01-01', {})".format(i)
            )

    instance.query("SYSTEM FLUSH DISTRIBUTED distributed")
    time.sleep(1.0)

    result = remote.query(
        "SELECT _part, groupArray(x) FROM local2 GROUP BY _part ORDER BY _part"
    )

    # Explanation: as merges are turned off on remote instance, active parts in local2 table correspond 1-to-1
    # to inserted blocks.
    # Batches of max 3 rows are formed as min_insert_block_size_rows = 3.
    # Blocks:
    # 1. Failed batch that is retried with the same contents.
    # 2. Full batch of inserts before ALTER.
    # 3. Full batch of inserts before ALTER.
    # 4. Full batch of inserts after ALTER (that have different block structure).
    # 5. What was left to insert with the column structure before ALTER.
    expected = """\
200001_1_1_0\t[1]
200001_2_2_0\t[2,3,4]
200001_3_3_0\t[5,6,7]
200001_4_4_0\t[10,11,12]
200001_5_5_0\t[8,9]
"""
    assert TSV(result) == TSV(expected)


def test_inserts_local(started_cluster):
    instance = instance_test_inserts_local_cluster
    instance.query("INSERT INTO distributed_on_local VALUES ('2000-01-01', 1)")
    time.sleep(0.5)
    assert instance.query("SELECT count(*) FROM local").strip() == "1"


def test_inserts_single_replica_local_internal_replication(started_cluster):
    with pytest.raises(
        QueryRuntimeException, match="Table default.single_replicated does not exist"
    ):
        node1.query(
            "INSERT INTO distributed_one_replica_internal_replication VALUES ('2000-01-01', 1)",
            settings={
                "distributed_foreground_insert": "1",
                "prefer_localhost_replica": "1",
                # to make the test more deterministic
                "load_balancing": "first_or_random",
            },
        )
    assert node2.query("SELECT count(*) FROM single_replicated").strip() == "0"


def test_inserts_single_replica_internal_replication(started_cluster):
    try:
        node1.query(
            "INSERT INTO distributed_one_replica_internal_replication VALUES ('2000-01-01', 1)",
            settings={
                "distributed_foreground_insert": "1",
                "prefer_localhost_replica": "0",
                # to make the test more deterministic
                "load_balancing": "first_or_random",
            },
        )
        assert node2.query("SELECT count(*) FROM single_replicated").strip() == "1"
    finally:
        node2.query("TRUNCATE TABLE single_replicated")


def test_inserts_single_replica_no_internal_replication(started_cluster):
    try:
        with pytest.raises(
            QueryRuntimeException,
            match="Table default.single_replicated does not exist",
        ):
            node1.query(
                "INSERT INTO distributed_one_replica_no_internal_replication VALUES ('2000-01-01', 1)",
                settings={
                    "distributed_foreground_insert": "1",
                    "prefer_localhost_replica": "0",
                },
            )
        assert node2.query("SELECT count(*) FROM single_replicated").strip() == "0"
    finally:
        node2.query("TRUNCATE TABLE single_replicated")


def test_prefer_localhost_replica(started_cluster):
    test_query = "SELECT * FROM distributed ORDER BY id"

    node1.query("INSERT INTO distributed VALUES (toDate('2017-06-17'), 11)")
    node2.query("INSERT INTO distributed VALUES (toDate('2017-06-17'), 22)")
    time.sleep(1.0)

    expected_distributed = """\
2017-06-17\t11
2017-06-17\t22
"""

    expected_from_node2 = """\
2017-06-17\t11
2017-06-17\t22
2017-06-17\t44
"""

    expected_from_node1 = """\
2017-06-17\t11
2017-06-17\t22
2017-06-17\t33
"""

    assert TSV(node1.query(test_query)) == TSV(expected_distributed)
    assert TSV(node2.query(test_query)) == TSV(expected_distributed)

    # Make replicas inconsistent by disabling merges and fetches
    #  for possibility of determining to which replica the query was send
    node1.query("SYSTEM STOP MERGES")
    node1.query("SYSTEM STOP FETCHES")
    node2.query("SYSTEM STOP MERGES")
    node2.query("SYSTEM STOP FETCHES")

    node1.query("INSERT INTO replicated VALUES (toDate('2017-06-17'), 33)")
    node2.query("INSERT INTO replicated VALUES (toDate('2017-06-17'), 44)")
    time.sleep(1.0)

    # Query is sent to node2, as it local and prefer_localhost_replica=1
    assert TSV(node2.query(test_query)) == TSV(expected_from_node2)

    # Now query is sent to node1, as it higher in order
    assert TSV(
        node2.query(
            test_query
            + " SETTINGS load_balancing='in_order', prefer_localhost_replica=0"
        )
    ) == TSV(expected_from_node1)


def test_inserts_low_cardinality(started_cluster):
    instance = shard1
    instance.query(
        "INSERT INTO low_cardinality_all (d,x,s) VALUES ('2018-11-12',1,'123')"
    )
    time.sleep(0.5)
    assert instance.query("SELECT count(*) FROM low_cardinality_all").strip() == "1"


def test_table_function(started_cluster):
    node1.query(
        "insert into table function cluster('shard_with_local_replica', 'default', 'table_function') select number, concat('str_', toString(number)) from numbers(100000)"
    )
    assert (
        node1.query(
            "select count() from cluster('shard_with_local_replica', 'default', 'table_function')"
        ).rstrip()
        == "100000"
    )
