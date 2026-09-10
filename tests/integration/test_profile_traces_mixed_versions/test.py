import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
main_configs = ["configs/remote_servers.xml", "configs/backups_disk.xml"]
coordinator = cluster.add_instance("coordinator", main_configs=main_configs, external_dirs=["/backups/"], with_zookeeper=True, use_keeper=False)
current = cluster.add_instance("current", main_configs=main_configs, external_dirs=["/backups/"], with_zookeeper=True, use_keeper=False)
older = cluster.add_instance(
    "older",
    main_configs=main_configs,
    external_dirs=["/backups/"],
    image="clickhouse/clickhouse-server",
    tag="26.5.1.882",
    stay_alive=True,
    with_installed_binary=True,
    with_zookeeper=True,
    use_keeper=False,
)
PEERS = {"older": older, "current": current}
SETTINGS = {
    "prefer_localhost_replica": 0,
    "use_hedged_requests": 0,
    "distributed_foreground_insert": 1,
    "query_profiler_cpu_time_period_ns": 0,
    "query_profiler_real_time_period_ns": 0,
    "memory_profiler_sample_probability": 0,
    "memory_profiler_step": 0,
    "max_execution_time": 20,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        assert older.query("SELECT version()") == "26.5.1.882\n"
        for node in (coordinator, current, older):
            node.query("INSERT INTO FUNCTION file('profile_traces.csv', 'CSV', 'x UInt8') VALUES (42)")
            # Separate replication groups exercise the replicated-destination forwarding
            # path without depending on part transfers between server versions.
            node.query(f"CREATE TABLE replicated_target (x UInt8) ENGINE = ReplicatedMergeTree('/profile_traces_mixed_versions/{node.name}', 'one') ORDER BY x")
        for name, peer in PEERS.items():
            peer.query("CREATE TABLE source (x UInt8) ENGINE = Memory")
            peer.query("INSERT INTO source VALUES (0), (1), (2)")
            peer.query("CREATE TABLE target (x UInt8) ENGINE = Memory")
            for node in (coordinator, peer):
                for table in ("source", "target"):
                    node.query(f"CREATE TABLE {table}_{name} (x UInt8) ENGINE = Distributed({name}_cluster, default, {table})")
        yield
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("carrier,mode", [("remote", 2), ("distributed", 1)], ids=["remote-2", "distributed-1"])
@pytest.mark.parametrize("value", [None, "0", "'false'", "1", "DEFAULT"])
def test_optimized_insert(peer_name, carrier, mode, value):
    peer = PEERS[peer_name]
    peer.query("TRUNCATE TABLE target")
    if carrier == "remote":
        target = f"FUNCTION remote('{peer_name}', default.target)"
        source = f"remote('{peer_name}', default.source)"
    else:
        target = f"target_{peer_name}"
        source = f"source_{peer_name}"
    trace_setting = "" if value is None else f", send_profile_traces = {value}"
    coordinator.query(
        f"INSERT INTO {target} SELECT x FROM {source} SETTINGS parallel_distributed_insert_select = {mode}{trace_setting}",
        settings=SETTINGS,
        timeout=30,
    )
    assert peer.query("SELECT x FROM target ORDER BY x") == "0\n1\n2\n"


@pytest.mark.parametrize("mode", [1, 2])
@pytest.mark.parametrize("value", ["0", "'false'"])
def test_promoted_view_opt_out(mode, value):
    current.query("TRUNCATE TABLE target")
    # The optimized `INSERT` promotes this view's `SELECT` to the forwarded query's
    # immediate source, where its opt-out must still override the outer opt-in.
    coordinator.query(
        "INSERT INTO FUNCTION remote('current', default.target) "
        "SELECT * FROM remote('current', view("
        "SELECT toUInt8(value) FROM system.settings "
        "WHERE name = 'send_profile_traces' "
        f"SETTINGS send_profile_traces = {value})) "
        f"SETTINGS parallel_distributed_insert_select = {mode}, send_profile_traces = 1",
        settings=SETTINGS,
        timeout=30,
    )
    assert current.query("SELECT x FROM target") == "0\n"


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("value", [None, "0", "'false'", "1", "DEFAULT"])
def test_file_cluster_select(peer_name, value):
    trace_setting = "" if value is None else f" SETTINGS send_profile_traces = {value}"
    assert (
        coordinator.query(
            f"SELECT x FROM fileCluster('{peer_name}_cluster', 'profile_traces.csv', 'CSV', 'x UInt8'){trace_setting}",
            settings=SETTINGS,
            timeout=30,
        )
        == "42\n"
    )


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("value", [None, "0", "'false'", "1", "DEFAULT"])
def test_replicated_insert_from_file_cluster(peer_name, value):
    peer = PEERS[peer_name]
    peer.query("TRUNCATE TABLE replicated_target")
    trace_setting = "" if value is None else f", send_profile_traces = {value}"
    coordinator.query(
        f"INSERT INTO replicated_target SELECT x FROM fileCluster('{peer_name}_cluster', 'profile_traces.csv', 'CSV', 'x UInt8') SETTINGS parallel_distributed_insert_select = 2{trace_setting}",
        settings=SETTINGS,
        timeout=30,
    )
    assert peer.query("SELECT x FROM replicated_target") == "42\n"


@pytest.mark.parametrize("mode", [1, 2])
@pytest.mark.parametrize("value", ["0", "'false'", "DEFAULT"])
def test_promoted_view_on_old_peer(mode, value):
    older.query("TRUNCATE TABLE target")
    # Removing the only entry in the view's SETTINGS must prune the clause itself.
    coordinator.query(
        "INSERT INTO FUNCTION remote('older', default.target) "
        f"SELECT * FROM remote('older', view(SELECT toUInt8(42) AS x SETTINGS send_profile_traces = {value})) "
        f"SETTINGS parallel_distributed_insert_select = {mode}, send_profile_traces = 1",
        settings=SETTINGS,
        timeout=30,
    )
    assert older.query("SELECT x FROM target") == "42\n"


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("value", ["0", "'false'"])
def test_authenticated_query_rewrite(peer_name, value):
    # The peer authenticates the transmitted SQL. Hashing the original text while
    # sending the legacy rewrite would reject this query before it can execute.
    assert (
        coordinator.query(
            f"SELECT x FROM fileCluster('{peer_name}_authenticated', 'profile_traces.csv', 'CSV', 'x UInt8') SETTINGS send_profile_traces = {value}",
            settings=SETTINGS,
            timeout=30,
        )
        == "42\n"
    )


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("value", ["0", "DEFAULT"])
def test_outer_select_setting(peer_name, value):
    assert (
        coordinator.query(
            f"SELECT x FROM remote('{peer_name}', default.source) ORDER BY x SETTINGS send_profile_traces = {value}",
            settings=SETTINGS,
            timeout=30,
        )
        == "0\n1\n2\n"
    )


@pytest.mark.parametrize("peer_name", ["older", "current"])
def test_setting_name_in_literal(peer_name):
    assert (
        coordinator.query(
            f"SELECT * FROM remote('{peer_name}', view(SELECT 'send_profile_traces = 0' AS text))",
            settings=SETTINGS,
            timeout=30,
        )
        == "send_profile_traces = 0\n"
    )


def test_initial_query_is_not_rewritten():
    error = older.query_and_get_error("SELECT 42 SETTINGS send_profile_traces = 0", timeout=30)
    assert "UNKNOWN_SETTING" in error and "send_profile_traces" in error


@pytest.fixture(scope="module")
def restore_seeds(started_cluster):
    backups = {}
    for name, peer in PEERS.items():
        peer.query("CREATE TABLE backup_source (x UInt8) ENGINE = Memory")
        peer.query("INSERT INTO backup_source VALUES (42)")
        backups[name] = f"Disk('backups', 'seed_{name}')"
        result = coordinator.query(
            f"BACKUP TABLE backup_source ON CLUSTER {name}_cluster TO {backups[name]}",
            settings=dict(SETTINGS, send_profile_traces=1),
            timeout=30,
        )
        # A delivery setting in the separate DDL settings payload is clamped by old workers.
        assert "BACKUP_CREATED" in result
    return backups


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("operation", ["BACKUP", "RESTORE"])
@pytest.mark.parametrize("index,value", enumerate([None, "0", "1", "'false'", "DEFAULT"]))
def test_backup_restore_query_setting(peer_name, operation, index, value, restore_seeds):
    name = f"backup_setting_{peer_name}_{index}"
    if operation == "BACKUP":
        query = f"BACKUP TABLE backup_source ON CLUSTER {peer_name}_cluster TO Disk('backups', '{name}')"
    else:
        query = f"RESTORE TABLE backup_source AS {name} ON CLUSTER {peer_name}_cluster FROM {restore_seeds[peer_name]}"
    if value is not None:
        query += f" SETTINGS send_profile_traces={value}"

    result = coordinator.query(query, settings=dict(SETTINGS, send_profile_traces=1), timeout=30)
    expected_status = "BACKUP_CREATED" if operation == "BACKUP" else "RESTORED"
    assert expected_status in result
    if operation == "RESTORE":
        assert PEERS[peer_name].query(f"SELECT x FROM {name}") == "42\n"


@pytest.mark.parametrize("peer_name", ["older", "current"])
@pytest.mark.parametrize("operation", ["BACKUP", "RESTORE"])
@pytest.mark.parametrize("clause", ["'not-a-bool'", "'not-a-bool', send_profile_traces=1", "1, send_profile_traces='not-a-bool'"])
def test_backup_restore_invalid_query_setting(peer_name, operation, clause, restore_seeds):
    if operation == "BACKUP":
        query = f"BACKUP TABLE backup_source ON CLUSTER {peer_name}_cluster TO Disk('backups', 'invalid_setting')"
    else:
        query = f"RESTORE TABLE backup_source AS invalid_restore ON CLUSTER {peer_name}_cluster FROM {restore_seeds[peer_name]}"
    # HTTP sends the invalid setting to the server without Native client validation.
    error = coordinator.http_query_and_get_error(
        query + f" SETTINGS send_profile_traces={clause}",
        method="POST",
        params=SETTINGS,
        timeout=30,
    )
    assert "CANNOT_PARSE_BOOL" in error
