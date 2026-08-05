import os
import random
import threading
import time

import pytest

from helpers.client import CommandRequest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/conf.d/merge_tree.xml", "configs/conf.d/remote_servers.xml"],
    with_zookeeper=True,
    macros={"layer": 0, "shard": 0, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/conf.d/merge_tree.xml", "configs/conf.d/remote_servers.xml"],
    with_zookeeper=True,
    macros={"layer": 0, "shard": 0, "replica": 2},
)
nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_random_inserts(started_cluster):
    # Duration of the test, reduce it if don't want to wait
    DURATION_SECONDS = 10  # * 60

    node1.query(
        """
        CREATE TABLE simple ON CLUSTER test_cluster (date Date, i UInt32, s String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY i"""
    )

    with PartitionManager() as pm_random_drops:
        for sacrifice in nodes:
            pass  # This test doesn't work with partition problems still
            # pm_random_drops._add_rule({'probability': 0.01, 'destination': sacrifice.ip_address, 'source_port': 2181, 'action': 'REJECT --reject-with tcp-reset'})
            # pm_random_drops._add_rule({'probability': 0.01, 'source': sacrifice.ip_address, 'destination_port': 2181, 'action': 'REJECT --reject-with tcp-reset'})

        min_timestamp = int(time.time())
        max_timestamp = min_timestamp + DURATION_SECONDS
        num_timestamps = max_timestamp - min_timestamp + 1

        bash_script = os.path.join(os.path.dirname(__file__), "test.sh")
        inserters = []
        for node in nodes:
            cmd = [
                "/bin/bash",
                bash_script,
                node.ip_address,
                str(min_timestamp),
                str(max_timestamp),
                str(cluster.get_client_cmd()),
            ]
            inserters.append(
                CommandRequest(cmd, timeout=DURATION_SECONDS * 2, stdin="")
            )
            print(node.name, node.ip_address)

        for inserter in inserters:
            inserter.get_answer()

    answer = "{}\t{}\t{}\t{}\n".format(
        num_timestamps, num_timestamps, min_timestamp, max_timestamp
    )

    for node in nodes:
        res = node.query_with_retry(
            "SELECT count(), uniqExact(i), min(i), max(i) FROM simple",
            check_callback=lambda res: TSV(res) == TSV(answer),
        )
        assert TSV(res) == TSV(answer), (
            node.name
            + " : "
            + node.query(
                "SELECT groupArray(_part), i, count() AS c FROM simple GROUP BY i ORDER BY c DESC LIMIT 1"
            )
        )

    node1.query("""DROP TABLE simple ON CLUSTER test_cluster""")


class Runner:
    def __init__(self):
        self.mtx = threading.Lock()
        self.total_inserted = 0
        self.inserted_vals = set()
        self.inserted_payloads = set()

        self.stop_ev = threading.Event()

    def do_insert(self, thread_num):
        self.stop_ev.wait(random.random())

        year = 2000
        month = "01"
        day = str(thread_num + 1).zfill(2)
        x = 1
        while not self.stop_ev.is_set():
            payload = """
{year}-{month}-{day}	{x1}
{year}-{month}-{day}	{x2}
""".format(
                year=year, month=month, day=day, x1=x, x2=(x + 1)
            ).strip()

            try:
                random.choice(nodes).query("INSERT INTO repl_test FORMAT TSV", payload)

                # print 'thread {}: insert {}, {}'.format(thread_num, i, i + 1)
                self.mtx.acquire()
                if payload not in self.inserted_payloads:
                    self.inserted_payloads.add(payload)
                    self.inserted_vals.add(x)
                    self.inserted_vals.add(x + 1)
                    self.total_inserted += 2 * x + 1
                self.mtx.release()

            except Exception as e:
                print("Exception:", e)

            x += 2
            self.stop_ev.wait(0.1 + random.random() / 10)


def test_insert_multithreaded(started_cluster):
    DURATION_SECONDS = 50

    for node in nodes:
        node.query("DROP TABLE IF EXISTS repl_test")

    for node in nodes:
        node.query(
            "CREATE TABLE repl_test(d Date, x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/repl_test', '{replica}') ORDER BY x PARTITION BY toYYYYMM(d)"
        )

    runner = Runner()

    threads = []
    for thread_num in range(5):
        threads.append(threading.Thread(target=runner.do_insert, args=(thread_num,)))

    for t in threads:
        t.start()

    time.sleep(DURATION_SECONDS)
    runner.stop_ev.set()

    for t in threads:
        t.join()

    # Sanity check: at least something was inserted
    assert runner.total_inserted > 0

    all_replicated = False
    for i in range(100):  # wait for replication 50 seconds max
        time.sleep(0.5)

        def get_delay(node):
            return int(
                node.query(
                    "SELECT absolute_delay FROM system.replicas WHERE table = 'repl_test'"
                ).rstrip()
            )

        if all([get_delay(n) == 0 for n in nodes]):
            all_replicated = True
            break

    assert all_replicated
    # Now we can be sure that all replicated fetches started, but they may not
    # be finished yet so we additionaly sync replicas, to be sure, that we have
    # all data on both replicas

    for node in nodes:
        node.query("SYSTEM SYNC REPLICA repl_test", timeout=10)

    actual_inserted = []
    for i, node in enumerate(nodes):
        actual_inserted.append(int(node.query("SELECT sum(x) FROM repl_test").rstrip()))
        assert actual_inserted[i] == runner.total_inserted
