import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/remap_executable.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_remap_executable(started_cluster):
    # The server started with `remap_executable = 1` and the default
    # `jemalloc_enable_background_threads = 1`, so the startup sequence had to
    # quiesce the signal listener thread, the asynchronous logging threads and
    # the jemalloc background threads around the remap window, and then start
    # them all again.
    assert node.query("SELECT 1") == "1\n"
    assert node.contains_in_log("Will remap executable in memory")
    assert node.contains_in_log("in memory has been successfully remapped")


def test_signal_listener_restarted(started_cluster):
    # SIGHUP is handled by the signal listener thread, which is stopped for the
    # remap window and restarted afterwards; the close-logs reaction proves the
    # restarted thread is alive and processing signals.
    pid = node.get_process_pid("clickhouse")
    node.exec_in_container(["bash", "-c", f"kill -HUP {pid}"], user="root")
    # The reopen archives clickhouse-server.log aside (`<rotateOnOpen>` in the common
    # integration config), so a line logged before the close can end up in the archived
    # file; only a line logged after the reopen is guaranteed to be in the tailed file.
    node.wait_for_log_line("Opened new log file after received signal")
    assert node.query("SELECT 1") == "1\n"


def test_only_signal_listener_takes_process_signals(started_cluster):
    # SIGHUP, SIGINT, SIGQUIT, SIGTERM and SIGCHLD are blocked in every thread but the
    # signal listener, which leaves them unblocked while it waits for the next record.
    # Every thread is created after the block: the remap re-creates the logging threads.
    process_signals = sum(1 << (sig - 1) for sig in (1, 2, 3, 15, 17))
    pid = node.get_process_pid("clickhouse")
    deadline = time.time() + 10
    while True:
        output = node.exec_in_container(
            [
                "bash",
                "-c",
                f"for t in /proc/{pid}/task/*; do m=$(awk '/^SigBlk/{{print $2}}' $t/status 2>/dev/null) && echo \"$m $(cat $t/comm 2>/dev/null)\"; done",
            ],
            user="root",
        )
        threads = [line.split(" ", 1) for line in output.splitlines() if line.strip()]
        listener = [int(mask, 16) & process_signals for mask, comm in threads if comm == "SignalListnr"]
        others = sorted(comm for mask, comm in threads if comm != "SignalListnr" and int(mask, 16) & process_signals != process_signals)
        if listener == [0] or time.time() > deadline:
            break
    assert listener == [0]
    assert others == []


def test_jemalloc_background_threads_restarted(started_cluster):
    # On builds with jemalloc, the background threads must be running again
    # after the remap quiesced them. Builds without jemalloc (sanitizers) do
    # not export the metric at all.
    has_jemalloc = node.query("SELECT count() FROM system.asynchronous_metrics WHERE metric = 'jemalloc.background_thread.num_threads'").strip() == "1"
    if not has_jemalloc:
        pytest.skip("this build has no jemalloc")

    deadline = time.time() + 60
    num_threads = 0
    while time.time() < deadline:
        num_threads = int(float(node.query("SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.background_thread.num_threads'").strip()))
        if num_threads > 0:
            break
        time.sleep(1)
    assert num_threads > 0
