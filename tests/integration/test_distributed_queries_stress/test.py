# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=line-too-long

import shlex
import itertools
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1_r1 = cluster.add_instance("node1_r1", main_configs=["configs/remote_servers.xml"])
node2_r1 = cluster.add_instance("node2_r1", main_configs=["configs/remote_servers.xml"])
node1_r2 = cluster.add_instance("node1_r2", main_configs=["configs/remote_servers.xml"])
node2_r2 = cluster.add_instance("node2_r2", main_configs=["configs/remote_servers.xml"])


def run_benchmark(payload, settings):
    node1_r1.exec_in_container(
        [
            "bash",
            "-c",
            "echo {} | ".format(shlex.quote(payload.strip()))
            + " ".join(
                [
                    "clickhouse",
                    "benchmark",
                    "--concurrency=100",
                    "--cumulative",
                    "--delay=0",
                    # NOTE: with current matrix even 3 seconds it huge...
                    "--timelimit=3",
                    # tune some basic timeouts
                    "--hedged_connection_timeout_ms=200",
                    "--connect_timeout_with_failover_ms=200",
                    "--connections_with_failover_max_tries=5",
                    *settings,
                ]
            ),
        ]
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for _, instance in cluster.instances.items():
            instance.query(
                """
            create table if not exists data (
                key Int,
                /* just to increase block size */
                v1 UInt64,
                v2 UInt64,
                v3 UInt64,
                v4 UInt64,
                v5 UInt64,
                v6 UInt64,
                v7 UInt64,
                v8 UInt64,
                v9 UInt64,
                v10 UInt64,
                v11 UInt64,
                v12 UInt64
            ) Engine=MergeTree() order by key partition by key%5;
            insert into data (key) select * from numbers(10);

            create table if not exists dist_one           as data engine=Distributed(one_shard, currentDatabase(), data, key);
            create table if not exists dist_one_over_dist as data engine=Distributed(one_shard, currentDatabase(), dist_one, kostikConsistentHash(key, 2));

            create table if not exists dist_two as data           engine=Distributed(two_shards, currentDatabase(), data, key);
            create table if not exists dist_two_over_dist as data engine=Distributed(two_shards, currentDatabase(), dist_two, kostikConsistentHash(key, 2));
            """
            )
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "table,settings",
    itertools.product(
        [  # tables
            "dist_one",
            "dist_one_over_dist",
            "dist_two",
            "dist_two_over_dist",
        ],
        [  # settings
            *list(
                itertools.combinations(
                    [
                        "",  # defaults
                        "--prefer_localhost_replica=0",
                        "--async_socket_for_remote=0",
                        "--use_hedged_requests=0",
                        "--optimize_skip_unused_shards=1",
                        "--distributed_group_by_no_merge=2",
                        "--optimize_distributed_group_by_sharding_key=1",
                        # TODO: enlarge test matrix (but first those values to accept ms):
                        #
                        # - sleep_in_send_tables_status
                        # - sleep_in_send_data
                    ],
                    2,
                )
            )
            # TODO: more combinations that just 2
        ],
    ),
)
def test_stress_distributed(table, settings, started_cluster):
    payload = f"""
    select * from {table} where key = 0;
    select * from {table} where key = 1;
    select * from {table} where key = 2;
    select * from {table} where key = 3;
    select * from {table};
    """
    run_benchmark(payload, settings)
