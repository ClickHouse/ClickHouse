# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time

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
        drop workload if exists a;
        drop workload if exists b;
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


def test_parked_query_still_renews_cpu_slot():
    node.query(
        """
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads = 1;
        create workload a in all;
        create workload b in all;
    """
    )
    node.query("drop table if exists parking_set_data sync")
    node.query(
        "create table parking_set_data (key UInt64, s String) engine = MergeTree order by key"
    )
    node.query(
        "insert into parking_set_data select number, randomPrintableASCII(100) from numbers(30000)"
    )

    # Query `a` parks its CPU slot on every throttled read while it builds the set, then runs on CPU for seconds
    query_a = (
        "select count() from numbers(600000000) where cityHash64(number) not in (select cityHash64(s) from parking_set_data) "
        "settings workload = 'a', max_threads = 1, max_local_read_bandwidth = 1000000, "
        "max_read_buffer_size_local_fs = 4096, min_bytes_to_use_direct_io = 1, local_filesystem_read_method = 'pread'"
    )

    errors_a = []

    def run_query_a():
        try:
            node.query(query_a, query_id="parking_a", timeout=300)
        except Exception as e:
            errors_a.append(str(e))

    thread_a = threading.Thread(target=run_query_a)
    thread_a.start()
    try:
        deadline = time.monotonic() + 120
        while True:
            rows = node.query(
                "select read_rows from system.processes where query_id = 'parking_a'"
            ).strip()
            if rows and int(rows) > 30000:
                break
            assert time.monotonic() < deadline, "query a did not build its set in time"
            time.sleep(0.05)

        node.query(
            "select count() from numbers(10000000) where cityHash64(number) % 3 = 0 settings workload = 'b', max_threads = 1",
            query_id="parking_b",
        )
        # `b` must have competed with a running `a`, or its short wait proves nothing
        assert (
            node.query("select count() from system.processes where query_id = 'parking_a'").strip() == "1"
        ), "query a stopped before query b finished"
    finally:
        node.query("kill query where query_id = 'parking_a' sync")
        thread_a.join()
    assert all("QUERY_WAS_CANCELLED" in e for e in errors_a), errors_a
    node.query("system flush logs")

    parks = int(
        node.query(
            "select ProfileEvents['ConcurrencyControlParks'] from system.query_log where query_id = 'parking_a' and type != 'QueryStart' "
            "order by query_start_time_microseconds desc limit 1"
        )
    )
    wait_us = get_profile_event("parking_b", "ConcurrencyControlWaitMicroseconds")
    print(f"parks of a: {parks}, CPU slot wait of b: {wait_us} us")

    assert parks > 100, f"expected parks > 100, got {parks}"
    # `a` has to renew its slot every 10 ms of CPU, so `b` waits about its own CPU time (up to ~0.5 s under sanitizers),
    # not a quantum per park of `a` (~5 s)
    assert (
        wait_us < 2000000
    ), f"query in workload b waited {wait_us} us for a CPU slot held by a query that parked {parks} times"
