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
    node.wait_for_log_line("Received signal to close logs")
    assert node.query("SELECT 1") == "1\n"


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
