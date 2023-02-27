import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# node1 -- distributed_directory_monitor_split_batch_on_failure=on
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/overrides_1.xml"],
)
# node2 -- distributed_directory_monitor_split_batch_on_failure=off
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/overrides_2.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for _, node in cluster.instances.items():
            node.query(
                """
            create table null_ (key Int, value Int) engine=Null();
            create table dist as null_ engine=Distributed(test_cluster, currentDatabase(), null_, key);
            create table data (key Int, uniq_values Int) engine=Memory();
            create materialized view mv to data as select key, uniqExact(value) uniq_values from null_ group by key;
            system stop distributed sends dist;

            create table dist_data as data engine=Distributed(test_cluster, currentDatabase(), data);
            """
            )

        yield cluster
    finally:
        cluster.shutdown()


def test_distributed_directory_monitor_split_batch_on_failure_OFF(started_cluster):
    for i in range(0, 100):
        limit = 100e3
        node2.query(
            f"insert into dist select number/100, number from system.numbers limit {limit} offset {limit*i}",
            settings={
                # max_memory_usage is the limit for the batch on the remote node
                # (local query should not be affected since 30MB is enough for 100K rows)
                "max_memory_usage": "30Mi",
                "max_untracked_memory": "0",
            },
        )
    # "Received from" is mandatory, since the exception should be thrown on the remote node.
    with pytest.raises(
        QueryRuntimeException,
        match=r"DB::Exception: Received from.*Memory limit \(for query\) exceeded: .*while pushing to view default\.mv",
    ):
        node2.query("system flush distributed dist")
    assert int(node2.query("select count() from dist_data")) == 0


def test_distributed_directory_monitor_split_batch_on_failure_ON(started_cluster):
    for i in range(0, 100):
        limit = 100e3
        node1.query(
            f"insert into dist select number/100, number from system.numbers limit {limit} offset {limit*i}",
            settings={
                # max_memory_usage is the limit for the batch on the remote node
                # (local query should not be affected since 30MB is enough for 100K rows)
                "max_memory_usage": "30Mi",
                "max_untracked_memory": "0",
            },
        )
    node1.query("system flush distributed dist")
    assert int(node1.query("select count() from dist_data")) == 100000
