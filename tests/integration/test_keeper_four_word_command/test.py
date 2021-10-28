import socket
import pytest
from helpers.cluster import ClickHouseCluster
import random
import string
import os
import time
from multiprocessing.dummy import Pool
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry
from io import StringIO
import csv

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/enable_keeper1.xml', 'configs/use_keeper.xml'],
                             stay_alive=True)
node2 = cluster.add_instance('node2', main_configs=['configs/enable_keeper2.xml', 'configs/use_keeper.xml'],
                             stay_alive=True)
node3 = cluster.add_instance('node3', main_configs=['configs/enable_keeper3.xml', 'configs/use_keeper.xml'],
                             stay_alive=True)

from kazoo.client import KazooClient, KazooState


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def wait_node(node):
    for _ in range(100):
        zk = None
        try:
            node.query("SELECT * FROM system.zookeeper WHERE path = '/'")
            zk = get_fake_zk(node.name, timeout=30.0)
            # zk.create("/test", sequence=True)
            print("node", node.name, "ready")
            break
        except Exception as ex:
            time.sleep(0.2)
            print("Waiting until", node.name, "will be ready, exception", ex)
        finally:
            destroy_zk_client(zk)
    else:
        raise Exception("Can't wait node", node.name, "to become ready")


def wait_nodes():
    for n in [node1, node2, node3]:
        wait_node(n)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout)
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_keeper_socket(nodename):
    hosts = cluster.get_instance_ip(nodename)
    client = socket.socket()
    client.settimeout(10)
    client.connect((hosts, 9181))
    return client


def close_keeper_socket(cli):
    if cli is not None:
        print("close socket")
        cli.close()


def test_cmd_ruok(started_cluster):
    client = None
    try:
        wait_nodes()
        client = get_keeper_socket("node1")
        client.send(b'ruok')
        data = client.recv(4)
        print(data)
        assert data.decode() == 'imok'
    finally:
        close_keeper_socket(client)


def do_some_action(zk, create_cnt=0, get_cnt=0, set_cnt=0, ephemeral_cnt=0, watch_cnt=0, delete_cnt=0):
    assert create_cnt >= get_cnt
    assert create_cnt >= set_cnt
    assert create_cnt >= watch_cnt
    assert create_cnt >= delete_cnt

    for i in range(create_cnt):
        zk.create("/normal_node_" + str(i), b"")

    for i in range(get_cnt):
        zk.get("/normal_node_" + str(i))

    for i in range(set_cnt):
        zk.set("/normal_node_" + str(i), b"new-value")

    for i in range(ephemeral_cnt):
        zk.create("/ephemeral_node_" + str(i), ephemeral=True)

    fake_ephemeral_event = None

    def fake_ephemeral_callback(event):
        print("Fake watch triggered")
        nonlocal fake_ephemeral_event
        fake_ephemeral_event = event

    for i in range(watch_cnt):
        zk.exists("/normal_node_" + str(i), watch=fake_ephemeral_callback)

    for i in range(create_cnt - delete_cnt, create_cnt):
        zk.delete("/normal_node_" + str(i))


def test_cmd_mntr(started_cluster):
    client = None
    zk = None
    try:
        wait_nodes()

        # reset stat first
        client = get_keeper_socket("node1")
        client.send(b'srst')
        data = client.recv(10_000)
        client.close()
        assert data.decode() == "Server stats reset."

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10, get_cnt=10, set_cnt=5, ephemeral_cnt=2, watch_cnt=2, delete_cnt=2)

        client = get_keeper_socket("node1")
        client.send(b'mntr')
        data = client.recv(10_000_000)
        assert len(data) != 0

        # print(data.decode())
        reader = csv.reader(data.decode().split('\n'), delimiter='\t')
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0]] = row[1]

        assert len(result["zk_version"]) != 0

        assert int(result["zk_avg_latency"]) >= 0
        assert int(result["zk_max_latency"]) >= 0
        assert int(result["zk_min_latency"]) >= 0

        assert int(result["zk_min_latency"]) <= int(result["zk_avg_latency"])
        assert int(result["zk_max_latency"]) >= int(result["zk_avg_latency"])

        assert int(result["zk_packets_received"]) == 31
        # contains 31 user request response, 1 session establish response
        assert int(result["zk_packets_sent"]) == 32

        assert 1 <= int(result["zk_num_alive_connections"]) <= 4
        assert int(result["zk_outstanding_requests"]) == 0

        assert result["zk_server_state"] == "leader"

        # contains:
        #   10 nodes created by test
        #   3 nodes created by clickhouse "/clickhouse/task_queue/ddl"
        #   1 root node
        assert int(result["zk_znode_count"]) == 14
        # ClickHouse may watch "/clickhouse/task_queue/ddl"
        assert int(result["zk_watch_count"]) >= 2
        assert int(result["zk_ephemerals_count"]) == 2
        assert int(result["zk_approximate_data_size"]) > 0

        assert int(result["zk_open_file_descriptor_count"]) > 0
        assert int(result["zk_max_file_descriptor_count"]) > 0

        assert int(result["zk_followers"]) == 2
        assert int(result["zk_synced_followers"]) == 2

    finally:
        destroy_zk_client(zk)
        close_keeper_socket(client)


def test_cmd_srst(started_cluster):
    client = None
    try:
        wait_nodes()

        # reset stat first
        client = get_keeper_socket("node1")
        client.send(b'srst')
        data = client.recv(10_000)
        client.close()
        assert data.decode() == "Server stats reset."

        client = get_keeper_socket("node1")
        client.send(b'mntr')
        data = client.recv(10_000_000)
        assert len(data) != 0

        # print(data.decode())
        reader = csv.reader(data.decode().split('\n'), delimiter='\t')
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0]] = row[1]

        assert int(result["zk_packets_received"]) == 0
        assert int(result["zk_packets_sent"]) == 0

    finally:
        close_keeper_socket(client)


def test_cmd_conf(started_cluster):
    client = None
    try:
        wait_nodes()
        client = get_keeper_socket("node1")

        # reset stat first
        client.send(b'conf')
        data = client.recv(10_000_000)
        assert len(data) != 0

        # print(data.decode())
        reader = csv.reader(data.decode().split('\n'), delimiter='=')
        result = {}

        for row in reader:
            if len(row) != 0:
                print(row)
                result[row[0]] = row[1]

        assert result["server_id"] == "1"
        assert result["tcp_port"] == "9181"
        assert "tcp_port_secure" not in result
        assert "superdigest" not in result

        assert result["log_storage_path"] == "/var/lib/clickhouse/coordination/log"
        assert result["snapshot_storage_path"] == "/var/lib/clickhouse/coordination/snapshots"

        assert result["session_timeout_ms"] == "10000"
        assert result["operation_timeout_ms"] == "5000"
        assert result["dead_session_check_period_ms"] == "500"
        assert result["heart_beat_interval_ms"] == "500"
        assert result["election_timeout_lower_bound_ms"] == "1000"
        assert result["election_timeout_upper_bound_ms"] == "2000"
        assert result["reserved_log_items"] == "100000"

        assert result["snapshot_distance"] == "75"
        assert result["auto_forwarding"] == "true"
        assert result["shutdown_timeout"] == "5000"
        assert result["startup_timeout"] == "180000"

        assert result["raft_logs_level"] == "trace"
        assert result["rotate_log_storage_interval"] == "100000"
        assert result["snapshots_to_keep"] == "3"
        assert result["stale_log_gap"] == "10000"
        assert result["fresh_log_gap"] == "200"

        assert result["max_requests_batch_size"] == "100"
        assert result["quorum_reads"] == "false"
        assert result["force_sync"] == "true"

        assert result["compress_logs"] == "true"
        assert result["compress_snapshots_with_zstd_format"] == "true"
        assert result["configuration_change_tries_count"] == "20"

    finally:
        close_keeper_socket(client)
