import csv
import re
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)

from kazoo.client import KazooClient


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])


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


def clear_znodes():
    zk = None
    try:
        zk = get_fake_zk(node3.name, timeout=30.0)
        nodes = zk.get_children("/")
        for node in [n for n in nodes if "test_4lw_" in n]:
            zk.delete("/" + node)
    finally:
        destroy_zk_client(zk)


def get_fake_zk(nodename, timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip(nodename) + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def close_keeper_socket(cli):
    if cli is not None:
        cli.close()


def reset_node_stats(node=node1):
    client = None
    try:
        client = keeper_utils.get_keeper_socket(cluster, node)
        client.send(b"srst")
        client.recv(10)
    finally:
        if client is not None:
            client.close()


def reset_conn_stats(node=node1):
    client = None
    try:
        client = keeper_utils.get_keeper_socket(cluster, node)
        client.send(b"crst")
        client.recv(10_000)
    finally:
        if client is not None:
            client.close()


def test_cmd_ruok(started_cluster):
    client = None
    try:
        wait_nodes()
        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="ruok")
        assert data == "imok"
    finally:
        close_keeper_socket(client)


def do_some_action(
    zk, create_cnt=0, get_cnt=0, set_cnt=0, ephemeral_cnt=0, watch_cnt=0, delete_cnt=0
):
    assert create_cnt >= get_cnt
    assert create_cnt >= set_cnt
    assert create_cnt >= watch_cnt
    assert create_cnt >= delete_cnt
    # ensure not delete watched node
    assert create_cnt >= (delete_cnt + watch_cnt)

    for i in range(create_cnt):
        zk.create("/test_4lw_normal_node_" + str(i), b"")

    for i in range(get_cnt):
        zk.get("/test_4lw_normal_node_" + str(i))

    for i in range(set_cnt):
        zk.set("/test_4lw_normal_node_" + str(i), b"new-value")

    for i in range(ephemeral_cnt):
        zk.create("/test_4lw_ephemeral_node_" + str(i), ephemeral=True)

    fake_ephemeral_event = None

    def fake_ephemeral_callback(event):
        print("Fake watch triggered")
        nonlocal fake_ephemeral_event
        fake_ephemeral_event = event

    for i in range(watch_cnt):
        zk.exists("/test_4lw_normal_node_" + str(i), watch=fake_ephemeral_callback)

    for i in range(create_cnt - delete_cnt, create_cnt):
        zk.delete("/test_4lw_normal_node_" + str(i))


def test_cmd_mntr(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        # reset stat first
        reset_node_stats(leader)

        zk = get_fake_zk(leader.name, timeout=30.0)
        do_some_action(
            zk,
            create_cnt=10,
            get_cnt=10,
            set_cnt=5,
            ephemeral_cnt=2,
            watch_cnt=2,
            delete_cnt=2,
        )

        data = keeper_utils.send_4lw_cmd(cluster, leader, cmd="mntr")

        # print(data.decode())
        reader = csv.reader(data.split("\n"), delimiter="\t")
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

        assert int(result["zk_num_alive_connections"]) == 1
        assert int(result["zk_outstanding_requests"]) == 0

        assert result["zk_server_state"] == "leader"

        # contains:
        #   10 nodes created by test
        #   3 nodes created by clickhouse "/clickhouse/task_queue/ddl"
        #   1 root node, 3 keeper system nodes
        assert int(result["zk_znode_count"]) == 14
        assert int(result["zk_watch_count"]) == 2
        assert int(result["zk_ephemerals_count"]) == 2
        assert int(result["zk_approximate_data_size"]) > 0

        assert int(result["zk_open_file_descriptor_count"]) > 0
        assert int(result["zk_max_file_descriptor_count"]) > 0

        assert int(result["zk_followers"]) == 2
        assert int(result["zk_synced_followers"]) == 2

        # contains 31 user request response and some responses for server startup
        assert int(result["zk_packets_sent"]) >= 31
        assert int(result["zk_packets_received"]) >= 31
    finally:
        destroy_zk_client(zk)


def test_cmd_srst(started_cluster):
    client = None
    try:
        wait_nodes()
        clear_znodes()

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="srst")
        assert data.strip() == "Server stats reset."

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="mntr")
        assert len(data) != 0

        # print(data)
        reader = csv.reader(data.split("\n"), delimiter="\t")
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
        clear_znodes()

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="conf")

        reader = csv.reader(data.split("\n"), delimiter="=")
        result = {}

        for row in reader:
            if len(row) != 0:
                print(row)
                result[row[0]] = row[1]

        assert result["server_id"] == "1"
        assert result["tcp_port"] == "9181"
        assert "tcp_port_secure" not in result
        assert "superdigest" not in result

        assert result["four_letter_word_allow_list"] == "*"
        assert result["log_storage_path"] == "/var/lib/clickhouse/coordination/log"
        assert result["log_storage_disk"] == "LocalLogDisk"
        assert (
            result["snapshot_storage_path"]
            == "/var/lib/clickhouse/coordination/snapshots"
        )
        assert result["snapshot_storage_disk"] == "LocalSnapshotDisk"

        assert result["session_timeout_ms"] == "30000"
        assert result["min_session_timeout_ms"] == "10000"
        assert result["operation_timeout_ms"] == "5000"
        assert result["dead_session_check_period_ms"] == "500"
        assert result["heart_beat_interval_ms"] == "500"
        assert result["election_timeout_lower_bound_ms"] == "1000"
        assert result["election_timeout_upper_bound_ms"] == "2000"
        assert result["leadership_expiry_ms"] == "0"
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
        assert result["max_requests_batch_bytes_size"] == "102400"
        assert result["max_flush_batch_size"] == "1000"
        assert result["max_request_queue_size"] == "100000"
        assert result["max_requests_quick_batch_size"] == "100"
        assert result["quorum_reads"] == "false"
        assert result["force_sync"] == "true"

        assert result["compress_logs"] == "false"
        assert result["compress_snapshots_with_zstd_format"] == "true"
        assert result["configuration_change_tries_count"] == "20"

        assert result["async_replication"] == "true"

        assert result["latest_logs_cache_size_threshold"] == "1073741824"
        assert result["commit_logs_cache_size_threshold"] == "524288000"

        assert result["disk_move_retries_wait_ms"] == "1000"
        assert result["disk_move_retries_during_init"] == "100"

        assert result["log_slow_total_threshold_ms"] == "5000"
        assert result["log_slow_cpu_threshold_ms"] == "100"
        assert result["log_slow_connection_operation_threshold_ms"] == "1000"
    finally:
        close_keeper_socket(client)


def test_cmd_isro(started_cluster):
    wait_nodes()
    assert keeper_utils.send_4lw_cmd(cluster, node1, "isro") == "rw"
    assert keeper_utils.send_4lw_cmd(cluster, node2, "isro") == "ro"


def test_cmd_srvr(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        reset_node_stats(leader)

        zk = get_fake_zk(leader.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = keeper_utils.send_4lw_cmd(cluster, leader, cmd="srvr")

        print("srvr output -------------------------------------")
        print(data)

        reader = csv.reader(data.split("\n"), delimiter=":")
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0].strip()] = row[1].strip()

        assert "ClickHouse Keeper version" in result
        assert "Latency min/avg/max" in result
        assert result["Received"] == "10"
        assert result["Sent"] == "10"
        assert int(result["Connections"]) == 1
        assert int(result["Zxid"], 16) >= 10
        assert result["Mode"] == "leader"
        assert result["Node count"] == "14"

    finally:
        destroy_zk_client(zk)


def test_cmd_stat(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        reset_node_stats(leader)
        reset_conn_stats(leader)

        zk = get_fake_zk(leader.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = keeper_utils.send_4lw_cmd(cluster, leader, cmd="stat")

        print("stat output -------------------------------------")
        print(data)

        # keeper statistics
        stats = [n for n in data.split("\n") if "=" not in n]
        reader = csv.reader(stats, delimiter=":")
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0].strip()] = row[1].strip()

        assert "ClickHouse Keeper version" in result
        assert "Latency min/avg/max" in result
        assert result["Received"] == "10"
        assert result["Sent"] == "10"
        assert int(result["Connections"]) == 1
        assert int(result["Zxid"], 16) >= 10
        assert result["Mode"] == "leader"
        assert result["Node count"] == "14"

        # filter connection statistics
        cons = [n for n in data.split("\n") if "=" in n]
        # filter connection created by 'cons'
        cons = [n for n in cons if "recved=0" not in n and len(n) > 0]
        assert len(cons) == 1

        conn_stat = re.match(r"(.*?)[:].*[(](.*?)[)].*", cons[0].strip(), re.S).group(2)
        assert conn_stat is not None

        result = {}
        for col in conn_stat.split(","):
            col = col.strip().split("=")
            result[col[0]] = col[1]

        assert result["recved"] == "10"
        assert result["sent"] == "10"

    finally:
        destroy_zk_client(zk)


def test_cmd_cons(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_conn_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="cons")

        print("cons output -------------------------------------")
        print(data)

        # filter connection created by 'cons'
        cons = [n for n in data.split("\n") if "recved=0" not in n and len(n) > 0]
        assert len(cons) == 1

        conn_stat = re.match(r"(.*?)[:].*[(](.*?)[)].*", cons[0].strip(), re.S).group(2)
        assert conn_stat is not None

        result = {}
        for col in conn_stat.split(","):
            col = col.strip().split("=")
            result[col[0]] = col[1]

        assert result["recved"] == "10"
        assert result["sent"] == "10"
        assert "sid" in result
        assert result["lop"] == "Create"
        assert "est" in result
        assert result["to"] == "30000"
        assert result["lcxid"] == "0x000000000000000a"
        assert "lzxid" in result
        assert "lresp" in result
        assert int(result["llat"]) >= 0
        assert int(result["minlat"]) >= 0
        assert int(result["avglat"]) >= 0
        assert int(result["maxlat"]) >= 0

    finally:
        destroy_zk_client(zk)


def test_cmd_crst(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_conn_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=10)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="crst")

        print("crst output -------------------------------------")
        print(data)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="cons")
        print("cons output(after crst) -------------------------------------")
        print(data)

        # 2 or 3 connections, 1 for 'crst', 1 for 'cons' command, 1 for zk
        # there can be a case when 'crst' connection is not cleaned before the cons call
        print("cons output(after crst) -------------------------------------")
        print(data)
        cons = [n for n in data.split("\n") if len(n) > 0]
        assert len(cons) == 2 or len(cons) == 3

        # connection for zk
        zk_conns = [n for n in cons if not n.__contains__("sid=0xffffffffffffffff")]

        # there can only be one
        assert len(zk_conns) == 1
        zk_conn = zk_conns[0]

        conn_stat = re.match(r"(.*?)[:].*[(](.*?)[)].*", zk_conn.strip(), re.S).group(2)
        assert conn_stat is not None

        result = {}
        for col in conn_stat.split(","):
            col = col.strip().split("=")
            result[col[0]] = col[1]

        assert result["recved"] == "0"
        assert result["sent"] == "0"
        assert "sid" in result
        assert result["lop"] == "NA"
        assert "est" in result
        assert result["to"] == "30000"
        assert "lcxid" not in result
        assert result["lzxid"] == "0xffffffffffffffff"
        assert result["lresp"] == "0"
        assert int(result["llat"]) == 0
        assert int(result["minlat"]) == 0
        assert int(result["avglat"]) == 0
        assert int(result["maxlat"]) == 0

    finally:
        destroy_zk_client(zk)


def test_cmd_dump(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, ephemeral_cnt=2)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="dump")

        print("dump output -------------------------------------")
        print(data)

        list_data = data.split("\n")

        session_count = int(re.match(r".*[(](.*?)[)].*", list_data[0], re.S).group(1))
        assert session_count == 1

        assert "\t" + "/test_4lw_ephemeral_node_0" in list_data
        assert "\t" + "/test_4lw_ephemeral_node_1" in list_data
    finally:
        destroy_zk_client(zk)


def test_cmd_wchs(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=2, watch_cnt=2)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="wchs")

        print("wchs output -------------------------------------")
        print(data)

        list_data = [n for n in data.split("\n") if len(n.strip()) > 0]

        # 37 connections watching 632141 paths
        # Total watches:632141
        matcher = re.match(
            r"([0-9].*) connections watching ([0-9].*) paths", list_data[0], re.S
        )
        conn_count = int(matcher.group(1))
        watch_path_count = int(matcher.group(2))
        watch_count = int(
            re.match(r"Total watches:([0-9].*)", list_data[1], re.S).group(1)
        )

        assert conn_count == 1
        assert watch_path_count == 2
        assert watch_count == 2
    finally:
        destroy_zk_client(zk)


def test_cmd_wchc(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=2, watch_cnt=2)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="wchc")

        print("wchc output -------------------------------------")
        print(data)

        list_data = [n for n in data.split("\n") if len(n.strip()) > 0]

        assert len(list_data) == 3
        assert "\t" + "/test_4lw_normal_node_0" in list_data
        assert "\t" + "/test_4lw_normal_node_1" in list_data
    finally:
        destroy_zk_client(zk)


def test_cmd_wchp(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()
        reset_node_stats()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=2, watch_cnt=2)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="wchp")

        print("wchp output -------------------------------------")
        print(data)

        list_data = [n for n in data.split("\n") if len(n.strip()) > 0]

        assert len(list_data) == 4
        assert "/test_4lw_normal_node_0" in list_data
        assert "/test_4lw_normal_node_1" in list_data
    finally:
        destroy_zk_client(zk)


def test_cmd_csnp(started_cluster):
    zk = None
    try:
        wait_nodes()
        zk = get_fake_zk(node1.name, timeout=30.0)
        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="csnp")

        print("csnp output -------------------------------------")
        print(data)

        try:
            int(data)
            assert True
        except ValueError:
            assert False
    finally:
        destroy_zk_client(zk)


def test_cmd_lgif(started_cluster):
    zk = None
    try:
        wait_nodes()
        clear_znodes()

        zk = get_fake_zk(node1.name, timeout=30.0)
        do_some_action(zk, create_cnt=100)

        data = keeper_utils.send_4lw_cmd(cluster, node1, cmd="lgif")

        print("lgif output -------------------------------------")
        print(data)

        reader = csv.reader(data.split("\n"), delimiter="\t")
        result = {}

        for row in reader:
            if len(row) != 0:
                result[row[0]] = row[1]

        assert int(result["first_log_idx"]) == 1
        assert int(result["first_log_term"]) == 1
        assert int(result["last_log_idx"]) >= 1
        assert int(result["last_log_term"]) == 1
        assert int(result["last_committed_log_idx"]) >= 1
        assert int(result["leader_committed_log_idx"]) >= 1
        assert int(result["target_committed_log_idx"]) >= 1
        assert int(result["last_snapshot_idx"]) >= 1
    finally:
        destroy_zk_client(zk)


def test_cmd_rqld(started_cluster):
    wait_nodes()
    # node2 can not be leader
    for node in [node1, node3]:
        data = keeper_utils.send_4lw_cmd(cluster, node, cmd="rqld")
        assert data == "Sent leadership request to leader."

        print("rqld output -------------------------------------")
        print(data)

        if not keeper_utils.is_leader(cluster, node):
            # pull wait to become leader
            retry = 0
            # TODO not a restrict way
            while not keeper_utils.is_leader(cluster, node) and retry < 30:
                time.sleep(1)
                retry += 1
            if retry == 30:
                print(
                    node.name
                    + " does not become leader after 30s, maybe there is something wrong."
                )
        assert keeper_utils.is_leader(cluster, node)


def test_cmd_clrs(started_cluster):
    if node1.is_built_with_sanitizer():
        return

    def get_memory_purges():
        return node1.query(
            "SELECT value FROM system.events WHERE event = 'MemoryAllocatorPurge' SETTINGS system_events_show_zero_values = 1"
        )

    zk = None
    try:
        wait_nodes()

        zk = get_fake_zk(node1.name, timeout=30.0)

        paths = [f"/clrs_{i}" for i in range(10000)]

        # we only count the events because we cannot reliably test memory usage of Keeper
        # but let's create and delete nodes so the first purge needs to release some memory
        create_transaction = zk.transaction()
        for path in paths:
            create_transaction.create(path)
        create_transaction.commit()

        delete_transaction = zk.transaction()
        for path in paths:
            delete_transaction.delete(path)
        delete_transaction.commit()

        # repeat multiple times to make sure MemoryAllocatorPurge isn't increased because of other reasons
        for _ in range(5):
            prev_purges = int(get_memory_purges())
            keeper_utils.send_4lw_cmd(cluster, node1, cmd="clrs")
            current_purges = int(get_memory_purges())
            assert current_purges > prev_purges
            prev_purges = current_purges

    finally:
        destroy_zk_client(zk)


def test_cmd_ydld(started_cluster):
    wait_nodes()
    for node in [node1, node3]:
        data = keeper_utils.send_4lw_cmd(cluster, node, cmd="ydld")
        assert data == "Sent yield leadership request to leader."

        print("ydld output -------------------------------------")
        print(data)

        # Whenever there is a leader switch, there is a brief amount of time when any
        # of the 4 letter commands will return empty result. Thus, we need to test for
        # negative condition. So we can't use keeper_utils.is_leader() here and likewise
        # in the while loop below.
        if not keeper_utils.is_follower(cluster, node):
            # wait for it to yield leadership
            retry = 0
            while not keeper_utils.is_follower(cluster, node) and retry < 30:
                time.sleep(1)
                retry += 1
            if retry == 30:
                print(
                    node.name
                    + " did not become follower after 30s of yielding leadership, maybe there is something wrong."
                )
        assert keeper_utils.is_follower(cluster, node)
