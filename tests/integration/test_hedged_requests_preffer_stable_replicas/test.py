import os
import sys
import time
import json

import pytest
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

NODES = defaultdict(lambda: [])


@pytest.fixture(scope="module")
def started_cluster():
    for shard_num in range(1, 3):
        for node_num in range(2):
            name = f"node_{shard_num}_{node_num}"
            NODES[shard_num].append(
                cluster.add_instance(
                    name,
                    user_configs=["configs/users.xml"],
                    with_zookeeper=True,
                    stay_alive=True,
                    main_configs=["configs/remote_servers.xml"],
                    macros={"shard": shard_num, "replica": name},
                )
            )
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()
        pass


QUERY_TIMING_BOUND = 10.0


def check_query_is_fast(node, query):
    t = query_get_execution_time(node, query)
    assert t < QUERY_TIMING_BOUND


def check_query_is_slow(node, query):
    t = query_get_execution_time(node, query)
    assert t > QUERY_TIMING_BOUND


def query_get_execution_time(node, query):
    res = json.loads(node.query(query + " FORMAT JSON"))
    print(res)
    return float(res["statistics"]["elapsed"])


@pytest.mark.parametrize("src_shard, dst_shard", [(1, 1), (1, 2)])
def test_stuck_replica(started_cluster, src_shard, dst_shard):
    if NODES[src_shard][0].is_built_with_thread_sanitizer():
        pytest.skip("Hedged requests don't work under Thread Sanitizer")

    ### Restarting all nodes first, to flush stable/unstable context to isolate the test.
    for [shard, hosts] in NODES.items():
        for host in hosts:
            host.restart_clickhouse()

    NODES[dst_shard][0].query(
        f"CREATE TABLE repl ON CLUSTER 'test_cluster' (a Int32) ENGINE = ReplicatedMergeTree('/test_stuck_replica/{{uuid}}/{{shard}}', '{{replica}}') ORDER BY a"
    )
    NODES[dst_shard][0].query(
        f"CREATE TABLE distr ON CLUSTER 'test_cluster' (a Int32) ENGINE = Distributed('test_cluster', default, repl)"
    )

    NODES[dst_shard][1].query("SYSTEM STOP FETCHES repl")
    NODES[dst_shard][0].query("INSERT INTO repl SELECT 1")

    with cluster.pause_container(NODES[dst_shard][0].name):
        time.sleep(10)
        NODES[dst_shard][1].query("SYSTEM START FETCHES repl")

        ### At this point we will see NO_REPLICA_HAS_PART exceptions.
        ### We are simulating case when one replica becomes unavailable immidealtly
        ### after new part commit and rest replicas can't fetch it.
        check_query_is_slow(
            NODES[src_shard][1],
            "SELECT a FROM distr SETTINGS max_replica_delay_for_distributed_queries=1",
        )
        check_query_is_fast(
            NODES[src_shard][1],
            "SELECT a FROM distr SETTINGS max_replica_delay_for_distributed_queries=1",
        )

    NODES[dst_shard][0].query("DROP TABLE distr ON CLUSTER 'test_cluster' NO DELAY")
    NODES[dst_shard][0].query("DROP TABLE repl ON CLUSTER 'test_cluster' NO DELAY")
