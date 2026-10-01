import uuid
import pytest
import logging
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/overrides.xml"], with_zookeeper=True, macros={"replica": "node1", "shard": "default"}, stay_alive=True)
node2 = cluster.add_instance("node2", main_configs=["configs/overrides.xml"], with_zookeeper=True, macros={"replica": "node2", "shard": "default"}, stay_alive=True)
node3 = cluster.add_instance("node3", main_configs=["configs/overrides.xml"], with_zookeeper=True, macros={"replica": "node3", "shard": "default"}, stay_alive=True)

primary_key_size = 0

def bootstrap():
    global primary_key_size

    for node in cluster.instances.values():
        # Note, distributed index analysis uses UUIDs, without Replicated database UUID will differs
        node.query("create database test engine=Replicated('/databases/test', '{replica}')")

    master = cluster.instances["node1"]
    master.query("""
        create table test.pk_test
        (
            k1 String,
            k2 String,
            k3 String,
            k4 UInt64,
            p Int
        )
        engine=ReplicatedMergeTree()
        order by (k1, k2, k3, k4)
        partition by (p)
        settings index_granularity=1, primary_key_lazy_load=0, distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0
        """)

    for node in cluster.instances.values():
        node.query("system stop merges test.pk_test")

    master.query("""
        insert into test.pk_test
        select
            repeat(char(reinterpretAsUInt8('a')+number%25), 100)::String,
            repeat(char(reinterpretAsUInt8('a')+number%25), 100)::String,
            repeat(char(reinterpretAsUInt8('a')+number%25), 100)::String,
            number,
            number%100
        from numbers_mt(3e6)
        settings parts_to_throw_insert=100
    """)

    parts = master.query("""
        select
            sum(primary_key_bytes_in_memory_allocated) pk,
            sum(marks_bytes) marks
            from system.parts
            where table = 'pk_test'
    """, parse=True).to_dict("records")[0]
    primary_key_size, marks_size = (int(parts['pk']), int(parts['marks']))
    # With index_granularity=1 will create PK of ~900MiB (300MiB per 1e6 rows)
    assert primary_key_size > 900e6

    logging.info("pk_test: PK: {} MiB, Marks: {} MiB", primary_key_size//(1024*1024*1024), marks_size//(1024*1024*1024))

    for node in cluster.instances.values():
        node.query("system sync replica test.pk_test")
    for node in cluster.instances.values():
        assert int(node.query("select count() from test.pk_test")) == 3e6


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        bootstrap()
        yield cluster
    finally:
        cluster.shutdown()


# Test to ensure that the PK is spreaded accross replicas
def test_primary_key():
    master = cluster.instances["node1"]

    for node in cluster.instances.values():
        node.query("system unload primary key test.pk_test")

    # Note, correctness (and ProfileEvents) checked in stateless tests
    master.query("select * from test.pk_test where k2 = repeat('a', 100)", settings={
        "distributed_index_analysis": 1,
        "distributed_index_analysis_for_non_shared_merge_tree": 1,
        "use_query_condition_cache": 0,
        "cluster_for_parallel_replicas": "default",
    })

    loaded_pk_df = master.query("""
        select hostName() host, sum(primary_key_bytes_in_memory_allocated) size
        from clusterAllReplicas(default, system.parts)
        where table = 'pk_test'
        group by host
    """, parse=True)
    assert loaded_pk_df["size"].max() < primary_key_size, loaded_pk_df
    assert loaded_pk_df["size"].mean() < primary_key_size/3*1.2 and loaded_pk_df["size"].mean() > primary_key_size/3*0.8, loaded_pk_df
    # CV
    assert loaded_pk_df["size"].std()/loaded_pk_df["size"].mean() < 0.2, loaded_pk_df


def _get_profile_event(node, query_id, event):
    """Return the value of a profile event for a given query_id from query_log."""
    node.query("system flush logs query_log")
    return int(node.query(f"""
        select ProfileEvents['{event}']
        from system.query_log
        where query_id = '{query_id}' and type = 'QueryFinish'
    """).strip())


def test_connection_reuse():
    """First DIA query must establish new connections; second must reuse them."""
    master = cluster.instances["node1"]
    master.restart_clickhouse()

    dia_settings = {
        "distributed_index_analysis": 1,
        "distributed_index_analysis_for_non_shared_merge_tree": 1,
        "use_query_condition_cache": 0,
        "cluster_for_parallel_replicas": "default",
    }

    qid1 = "dia_connect_" + str(uuid.uuid4())
    master.query(
        "select * from test.pk_test where k2 = repeat('a', 100)",
        query_id=qid1,
        settings=dia_settings,
    )
    connects1 = _get_profile_event(master, qid1, "DistributedConnectionConnectCount")
    assert connects1 == 2, f"First query should establish connections, got Connects={connects1}"

    qid2 = "dia_connect_" + str(uuid.uuid4())
    master.query(
        "select * from test.pk_test where k2 = repeat('a', 100)",
        query_id=qid2,
        settings=dia_settings,
    )
    connects2 = _get_profile_event(master, qid2, "DistributedConnectionConnectCount")
    assert connects2 == 0, f"Second query should reuse connections, got Connects={connects2}"
