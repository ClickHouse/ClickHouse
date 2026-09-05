import concurrent.futures
import os
import re
import time


def generate_keeper_configs(configs_dir, clusters):
    """Generate Keeper XML config files for the given cluster definitions.

    clusters is a list of (filenames, hosts, chunk_size[, use_s3_disk_primary]) tuples where:
      - filenames:           output XML file names, one per server
      - hosts:               hostname for each server (same length as filenames)
      - chunk_size:          snapshot_transfer_chunk_size value, or None to omit
      - use_s3_disk_primary: (optional, default False) use an S3 plain disk as the *primary*
                             snapshot disk so that RemoteSnapshotLoader is exercised during
                             chunked transfer. Each server gets its own prefix inside the
                             "root" bucket (pre-created by the test framework).
    """
    def make_config(server_id, hosts, chunk_size, use_s3_disk_primary):
        if use_s3_disk_primary:
            # Each server gets its own S3 path so snapshot objects don't collide across clusters.
            endpoint = f"http://minio1:9001/root/keeper-snapshots/{hosts[server_id - 1]}/"
            storage_block = (
                "\n<storage_configuration>"
                "\n    <disks>"
                "\n        <keeper_snap_s3>"
                "\n            <type>s3_plain</type>"
                f"\n            <endpoint>{endpoint}</endpoint>"
                "\n            <access_key_id>minio</access_key_id>"
                "\n            <secret_access_key>ClickHouse_Minio_P@ssw0rd</secret_access_key>"
                "\n        </keeper_snap_s3>"
                "\n    </disks>"
                "\n</storage_configuration>"
            )
            snapshot_disk_line = "\n        <snapshot_storage_disk>keeper_snap_s3</snapshot_storage_disk>"
        else:
            storage_block = ""
            snapshot_disk_line = ""

        chunk_line = (
            f"\n            <snapshot_transfer_chunk_size>{chunk_size}</snapshot_transfer_chunk_size>"
            if chunk_size else ""
        )
        # Assign decreasing priorities: first node is most likely to become leader.
        base_prios = [70, 20, 10, 5, 3]
        prios = base_prios[:len(hosts)]
        servers = []
        for i, (host, prio) in enumerate(zip(hosts, prios), start=1):
            follower = "\n                <start_as_follower>true</start_as_follower>" if i > 1 else ""
            servers.append(
                f"            <server>\n"
                f"                <id>{i}</id>\n"
                f"                <hostname>{host}</hostname>\n"
                f"                <port>9234</port>\n"
                f"                <can_become_leader>true</can_become_leader>{follower}\n"
                f"                <priority>{prio}</priority>\n"
                f"            </server>"
            )
        return (
            "<clickhouse>\n"
            + (f"{storage_block}\n" if storage_block else "")
            + f"    <keeper_server>{snapshot_disk_line}\n"
            f"        <tcp_port>9181</tcp_port>\n"
            f"        <server_id>{server_id}</server_id>\n"
            f"        <data_storage_path>/var/lib/clickhouse/coordination/data</data_storage_path>\n"
            f"\n"
            f"        <coordination_settings>\n"
            f"            <operation_timeout_ms>5000</operation_timeout_ms>\n"
            f"            <session_timeout_ms>10000</session_timeout_ms>\n"
            f"            <raft_logs_level>trace</raft_logs_level>\n"
            f"            <snapshot_distance>50</snapshot_distance>\n"
            f"            <stale_log_gap>10</stale_log_gap>\n"
            f"            <reserved_log_items>1</reserved_log_items>{chunk_line}\n"
            f"        </coordination_settings>\n"
            f"\n"
            f"        <raft_configuration>\n"
            + "\n".join(servers) + "\n"
            "        </raft_configuration>\n"
            "    </keeper_server>\n"
            "</clickhouse>\n"
        )

    os.makedirs(configs_dir, exist_ok=True)
    # Always write the small remote read-buffer config so that ReadBufferFromS3::nextImpl
    # is called multiple times per readStrict, making failpoints reachable and stressing
    # RemoteSnapshotLoader under realistic multi-chunk I/O conditions.
    small_buf_path = os.path.join(configs_dir, "small_remote_buf_user.xml")
    with open(small_buf_path, "w") as f:
        f.write(
            "<clickhouse>\n<profiles>\n    <default>\n"
            "        <max_read_buffer_size_remote_fs>1024</max_read_buffer_size_remote_fs>\n"
            "    </default>\n</profiles>\n</clickhouse>\n"
        )
    for cluster_def in clusters:
        filenames, hosts, chunk_size = cluster_def[:3]
        use_s3_disk_primary = cluster_def[3] if len(cluster_def) > 3 else False
        for server_id, filename in enumerate(filenames, start=1):
            path = os.path.join(configs_dir, filename)
            with open(path, "w") as f:
                f.write(make_config(server_id, hosts, chunk_size, use_s3_disk_primary))


def stop_zk(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except Exception:
        pass


def fill_test_tree(zk, base, count=300):
    import os as _os
    zk.ensure_path(base)
    for i in range(count):
        zk.create(f"{base}/{i}", _os.urandom(1024))  # random to resist ZSTD compression
    for i in range(0, count, 10):
        zk.delete(f"{base}/{i}")


def cleanup_test_tree(cluster, leader_node, base):
    import helpers.keeper_utils as keeper_utils
    zk = None
    try:
        zk = keeper_utils.get_fake_zk(cluster, leader_node.name)
        if zk.exists(base):
            zk.delete(base, recursive=True)
    except Exception:
        pass
    finally:
        stop_zk(zk)


def verify_test_tree(leader_zk, lagging_zk, base, count=300):
    leader_zk.sync(base)
    lagging_zk.sync(base)
    for i in range(count):
        if i % 10 != 0:
            assert lagging_zk.get(f"{base}/{i}")[0] == leader_zk.get(f"{base}/{i}")[0]
        else:
            assert lagging_zk.exists(f"{base}/{i}") is None


def get_kill_timestamp(node):
    return node.query("SELECT now64(6)").strip()


def _query_text_log(node, after_time, pattern, timeout=15):
    deadline = time.time() + timeout
    while True:
        try:
            node.query("SYSTEM FLUSH LOGS")
            result = node.query(
                f"SELECT message FROM system.text_log "
                f"WHERE event_time_microseconds > '{after_time}' "
                f"AND message LIKE '{pattern}' "
                f"ORDER BY event_time_microseconds"
            ).strip()
            if result:
                return [line for line in result.splitlines() if line]
        except Exception:
            pass

        if time.time() >= deadline:
            return []
        time.sleep(1)


def get_received_snapshot_info(node, after_time, timeout=15):
    lines = _query_text_log(node, after_time, "Saved snapshot % chunks, % bytes)", timeout)
    if not lines:
        return None
    m = re.search(r"Saved snapshot (\d+) \((\d+) chunks, (\d+) bytes\)", lines[-1])
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def get_snapshot_log_lines_for_idx(node, snapshot_log_idx, after_time, timeout=15):
    return _query_text_log(
        node, after_time, f"Saving snapshot {snapshot_log_idx} obj_id %", timeout
    )


def assert_receiving_snapshot_logged(node_lagging, after_time, disk_type):
    """Assert that the follower logged receiving a snapshot to the expected disk type ("local" or "remote")."""
    pattern = f"Receiving snapshot % to {disk_type} disk"
    lines = _query_text_log(node_lagging, after_time, pattern, timeout=15)
    assert lines, f"Expected '{pattern}' in system.text_log on {node_lagging.name}"


def assert_obj_ids(node_lagging, snapshot_log_idx, expected, after_time):
    lines = get_snapshot_log_lines_for_idx(node_lagging, snapshot_log_idx, after_time)
    assert lines, "No 'Saving snapshot' log lines appeared during recovery"
    all_ids = [int(m.group(1)) for line in lines if (m := re.search(r"obj_id (\d+)", line))]
    duplicates = len(all_ids) - len(set(all_ids))
    # NuRaft may re-send a chunk when a heartbeat fires before the first ACK returns;
    # tolerate at most len(expected)//2 duplicates to catch systematic bugs.
    max_allowed = len(expected) // 2
    assert set(all_ids) == set(expected), f"Expected obj_ids={set(expected)}, got: {sorted(set(all_ids))}"
    assert duplicates <= max_allowed, \
        f"Too many duplicate chunks: {duplicates} (max {max_allowed}), obj_ids={all_ids}"


def phase_instances(phase_clusters, phase=None):
    """Instances of one phase of a phase table, or of all of them when phase is None.

    A phase table maps a phase name to the list of independent Keeper clusters that
    phase exercises: {"small": [[node1, node2, node3], [node7, node8, node9]], ...}.
    """
    phases = phase_clusters.values() if phase is None else [phase_clusters[phase]]
    return [node for clusters in phases for cluster_nodes in clusters for node in cluster_nodes]


def _wait_serving_requests(cluster, node, timeout):
    import helpers.keeper_utils as keeper_utils
    # A server started a moment ago has not necessarily bound its Keeper port yet, and
    # wait_until_connected lets the refused connect propagate. Complete readiness is not
    # probed because that probe is a client read, which a pinned old image can drop.
    deadline = time.time() + timeout
    while True:
        try:
            keeper_utils.wait_until_connected(cluster, node, wait_complete_readiness=False)
            return
        except OSError:
            if time.time() >= deadline:
                raise
            time.sleep(0.5)


def _wait_first_node_is_leader(cluster, nodes, timeout):
    import helpers.keeper_utils as keeper_utils
    # generate_keeper_configs gives the first server of a cluster the top priority and starts
    # every other one as a follower, and no test kills the first server. Leadership has to be
    # back on the first server before a test runs, or killing a lagging node can remove the
    # leader instead, and the re-election that follows drops the writes then in flight.
    deadline = time.time() + timeout
    last_request = None
    while True:
        try:
            if keeper_utils.is_leader(cluster, nodes[0]):
                return
            if last_request is None or time.time() - last_request > 5:
                keeper_utils.send_4lw_cmd(cluster, nodes[0], cmd="rqld")
                last_request = time.time()
        except OSError:
            pass
        if time.time() >= deadline:
            raise Exception(
                f"{nodes[0].name} did not become the Keeper leader within {timeout}s"
            )
        time.sleep(0.5)


def use_keeper_phase(cluster, phase_clusters, phase, start_timeout=180, always_running=()):
    """Leave exactly the Keeper clusters of `phase` serving and led by their first server,
    and every other instance of the phase table stopped.

    Only one phase is ever under test, while an idle sanitizer server costs about as
    much resident memory as a busy one, so the rest stay stopped instead of resident.

    Instances in `always_running` are never stopped or started, only asserted to be up.
    """
    wanted = {node.name for node in phase_instances(phase_clusters, phase)}
    pinned = {node.name for node in always_running}

    # Converge from live process state rather than from a remembered phase: a test that
    # fails between its own stop_clickhouse() and start_clickhouse() leaves a node of
    # the current phase down, and the next test in that phase needs it back.
    to_start = []
    for node in phase_instances(phase_clusters):
        if node.name in pinned:
            continue
        running = node.get_process_pid("clickhouse") is not None
        if node.name not in wanted:
            if running:
                node.stop_clickhouse()
        elif not running:
            to_start.append(node)

    # A Keeper server brought up on its own stalls for keeper_server.startup_timeout
    # waiting for a quorum only its peers can form, and start_clickhouse waits for the
    # server to answer queries, so the servers of a phase have to come up together.
    if to_start:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(to_start)) as pool:
            futures = [pool.submit(node.start_clickhouse, start_timeout) for node in to_start]
            for future in concurrent.futures.as_completed(futures):
                future.result()

    for cluster_nodes in phase_clusters[phase]:
        for node in cluster_nodes:
            _wait_serving_requests(cluster, node, start_timeout)
        _wait_first_node_is_leader(cluster, cluster_nodes, start_timeout)

    for node in phase_instances(phase_clusters):
        running = node.get_process_pid("clickhouse") is not None
        expected = node.name in wanted or node.name in pinned
        assert running == expected, (
            f"{node.name} is {'running' if running else 'stopped'} but Keeper phase "
            f"'{phase}' requires it to be {'running' if expected else 'stopped'}"
        )


def start_keeper_phase_only(cluster, phase_clusters, phase, always_running=()):
    """Stop every server outside `phase` right after cluster.start() brought them all up."""
    keep = {node.name for node in phase_instances(phase_clusters, phase)}
    keep |= {node.name for node in always_running}
    for node in phase_instances(phase_clusters):
        if node.name not in keep:
            node.stop_clickhouse()
