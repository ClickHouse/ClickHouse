# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[
        "configs/00_cpu_slot_parking.xml",
    ],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def clear_workloads_and_resources():
    node.query(
        """
        drop workload if exists all;
        drop resource if exists cpu;
    """
    )
    yield


@pytest.fixture(scope="function")
def with_custom_config(request):
    for name, server_settings in request.param.items():
        inst = cluster.instances[name]
        xml = "".join(f"<{k}>{v}</{k}>" for k, v in server_settings.items())
        inst.exec_in_container(
            [
                "bash",
                "-c",
                f"echo '<clickhouse>{xml}</clickhouse>' > /etc/clickhouse-server/config.d/99-custom_config.xml",
            ]
        )
        inst.query("system reload config")
    yield
    for name, server_settings in request.param.items():
        inst = cluster.instances[name]
        inst.exec_in_container(
            ["bash", "-c", "rm -f /etc/clickhouse-server/config.d/99-custom_config.xml"]
        )
        inst.query("system reload config")


def get_profile_event(query_id, profile_event):
    return int(
        node.query(
            f"select ProfileEvents['{profile_event}'] from system.query_log where current_database = currentDatabase() and query_id = '{query_id}' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
        )
    )


def setup_cpu_workload():
    # Binding the `cpu` resource to master+worker threads routes the query's CPU slots through
    # the workload scheduler (CPULeaseAllocation). Parking only happens on that path.
    node.query(
        """
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=8;
    """
    )


def create_throttled_table():
    node.query("drop table if exists park_data sync")
    node.query(
        "create table park_data (key UInt64, value String) engine=MergeTree order by key"
    )
    # Poorly-compressible payload so the on-disk bytes are real and the read-bandwidth
    # throttler actually sleeps -- that sleep is what parks the CPU lease.
    node.query(
        "insert into park_data select number, randomPrintableASCII(150) from numbers(300000)"
    )


# A low local read bandwidth forces Throttler::sleep on the pipeline worker threads while they
# hold a CPU lease, which parks the lease (releasing the CPU slot) and unparks it on wakeup.
THROTTLED_QUERY = (
    "select sum(length(value)) from park_data "
    "settings workload = 'all', max_local_read_bandwidth = 8000000, max_threads = 4"
)


def test_parking_fires_on_throttled_read():
    setup_cpu_workload()
    create_throttled_table()
    query_id = "cpu_parking_on"
    node.query(THROTTLED_QUERY, query_id=query_id)
    node.query("system flush logs")
    parks = get_profile_event(query_id, "ConcurrencyControlParks")
    unparks = get_profile_event(query_id, "ConcurrencyControlUnparks")
    # The throttled read sleeps repeatedly, so the lease parks and unparks many times.
    assert parks > 0, f"expected parks > 0, got {parks}"
    assert unparks > 0, f"expected unparks > 0, got {unparks}"
    # Every park is matched by exactly one unpark within the query.
    assert parks == unparks, f"parks={parks} != unparks={unparks}"


# A self-`remote` read makes the coordinator's single (max_threads=1) pipeline thread wait on an
# async socket. That wait happens inside PollingQueue's epoll section, which is a different park
# site from the throttled-local-read test above (Throttler::sleep): it is the single-thread
# async-wait path, where the executor blocks in `ExecutorTasks::tryGetTask` while holding a lease.
# Throttling the remote side's local read keeps the async wait long enough to park repeatedly.
ASYNC_REMOTE_QUERY = (
    "select sum(length(value)) from remote('127.0.0.2', currentDatabase(), park_data) "
    "settings workload = 'all', max_threads = 1, async_socket_for_remote = 1, "
    "async_query_sending_for_remote = 1, max_local_read_bandwidth = 4000000"
)


def test_parking_fires_on_single_thread_async_remote_wait():
    setup_cpu_workload()
    create_throttled_table()
    query_id = "cpu_parking_async_remote"
    node.query(ASYNC_REMOTE_QUERY, query_id=query_id)
    node.query("system flush logs")
    parks = get_profile_event(query_id, "ConcurrencyControlParks")
    unparks = get_profile_event(query_id, "ConcurrencyControlUnparks")
    # The coordinator's single thread waits on the remote socket (async) via PollingQueue while
    # holding a CPU lease, so the lease parks and unparks. This exercises the max_threads=1
    # single-thread async-wait path, not the multi-thread idle wait.
    assert parks > 0, f"expected parks > 0, got {parks}"
    assert unparks > 0, f"expected unparks > 0, got {unparks}"
    assert parks == unparks, f"parks={parks} != unparks={unparks}"


# Reproduce the #95727 idle-worker park deterministically and cheaply. A parallel `numbers_mt`
# branch keeps several workers busy long enough to be spawned; a serial `numbers` branch (a single
# stream) then sleeps for a few seconds while those workers have no task, so they sleep in
# `ExecutorTasks::tryGetTask` -- the idle wait -- and park. The sleeps make the idle phase long in
# wall-clock time rather than row count, so it is hardware-independent and fast under any sanitizer
# (a large window query was either flaky on fast CI or timed out under msan). No table/throttle/
# async/cache path is touched, so the only park site reached is the idle wait, isolating that guard.
IDLE_WORKER_QUERY = (
    "select count() from ("
    "  select sleepEachRow(0.2) from numbers_mt(80)"
    "  union all"
    "  select sleepEachRow(0.5) from numbers(10)"
    ") settings workload = 'all', max_threads = 8, max_block_size = 1"
)


def test_parking_fires_on_idle_worker():
    setup_cpu_workload()
    query_id = "cpu_parking_idle"
    node.query(IDLE_WORKER_QUERY, query_id=query_id)
    node.query("system flush logs")
    parks = get_profile_event(query_id, "ConcurrencyControlParks")
    unparks = get_profile_event(query_id, "ConcurrencyControlUnparks")
    # Workers idle during the serial sleep phase park via the idle wait (#95727), so parking fires
    # even with no I/O throttling or async wait involved.
    assert parks > 0, f"expected parks > 0, got {parks}"
    assert unparks > 0, f"expected unparks > 0, got {unparks}"
    assert parks == unparks, f"parks={parks} != unparks={unparks}"


@pytest.mark.parametrize(
    "with_custom_config",
    [
        pytest.param(
            {"node": {"cpu_slot_parking": "false"}},
            id="cpu-slot-parking-disabled",
        )
    ],
    indirect=True,
)
def test_parking_disabled_no_parks(with_custom_config):
    setup_cpu_workload()
    create_throttled_table()
    query_id = "cpu_parking_off"
    node.query(THROTTLED_QUERY, query_id=query_id)
    node.query("system flush logs")
    # With cpu_slot_parking disabled the executor never publishes the lease, so the same
    # throttled read must not park at all (zero-overhead-when-off gate).
    assert get_profile_event(query_id, "ConcurrencyControlParks") == 0
