import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
coordinator = cluster.add_instance("coordinator", main_configs=["configs/remote_servers.xml"])
current = cluster.add_instance("current", main_configs=["configs/remote_servers.xml"])
older = cluster.add_instance(
    "older",
    main_configs=["configs/remote_servers.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.5.1.882",
    stay_alive=True,
    with_installed_binary=True,
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
