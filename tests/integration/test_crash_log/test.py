import mmap
import os
import shutil
import time

import pytest

import helpers.cluster
import helpers.test_tools

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_node():
    cluster = helpers.cluster.ClickHouseCluster(__file__)
    node = cluster.add_instance(
        "node",
        main_configs=[
            "configs/crash_log.xml",
            "configs/core_dump.xml",
            "configs/disable_trace_log.xml",
        ],
        env_variables={
            "ASAN_OPTIONS": "use_sigaltstack=0 disable_coredump=0",
            "TSAN_OPTIONS": "use_sigaltstack=0 memory_limit_mb=5120 disable_coredump=0",
            "MSAN_OPTIONS": "disable_coredump=0",
        },
        stay_alive=True,
    )
    try:
        cluster.start()
        yield node
    finally:
        shutil.rmtree(os.path.join(node.path, "database", "cores"), ignore_errors=True)
        cluster.shutdown(ignore_fatal=True, ignore_sanitizer=True)


def send_signal(started_node, signal):
    pid = started_node.get_process_pid("clickhouse")
    started_node.exec_in_container(
        ["bash", "-c", f"kill -{signal} {pid}"], user="root"
    )


def wait_for_clickhouse_stop(started_node):
    result = None
    ## The signal handler thread waits up to ~303s before killing the process
    ## (300s polling for fatal_error_printed + 3s extra sleep), so we need to
    ## wait at least that long. On loaded CI machines, the crash handler can
    ## take over 180s due to stack trace symbolization and the
    ## sleep_in_logs_flush failpoint adding 30s per log flush.
    for attempt in range(360):
        time.sleep(1)
        pid = started_node.get_process_pid("clickhouse")
        if pid is None:
            result = "OK"
            break
    assert result == "OK", "ClickHouse process is still running"


def test_crash_log_synchronous(started_node):
    started_node.query("TRUNCATE TABLE IF EXISTS system.crash_log")

    crashes_count = 0
    for signal in ["SEGV", "4"]:
        started_node.query("SYSTEM ENABLE FAILPOINT sleep_in_logs_flush")
        send_signal(started_node, signal)
        wait_for_clickhouse_stop(started_node)
        started_node.restart_clickhouse()
        crashes_count += 1
        assert (
            started_node.query("SELECT COUNT(*) FROM system.crash_log")
            == f"{crashes_count}\n"
        )


@pytest.mark.parametrize(
    "failpoint, trace_column",
    [
        ("terminate_with_exception", "current_exception_trace_full"),
        ("terminate_with_std_exception", "current_exception_trace_full"),
        ("terminate_with_exception", "trace_full"),
        ("terminate_with_std_exception", "trace_full"),
        ("libcxx_hardening_out_of_bounds_assertion", "trace_full"),
    ]
)
def test_crash_log_extra_fields(started_node, failpoint, trace_column):
    started_node.query("TRUNCATE TABLE IF EXISTS system.crash_log")
    started_node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    started_node.query("SELECT 1", ignore_error=True)
    wait_for_clickhouse_stop(started_node)
    started_node.restart_clickhouse()

    assert started_node.query(
        f"""
        SELECT
            count()
        FROM system.crash_log
        WHERE 1
            AND signal = 6
            AND signal_code = -6 -- SI_TKILL
            AND signal_description = 'Sent by tkill.'
            AND fault_access_type = ''
            AND fault_address IS NULL
            AND arrayExists(x -> x LIKE '%executeQuery%', {trace_column})
            AND query = 'SELECT 1'
            AND length(git_hash) > 0
            AND length(architecture) > 0
        """
    ).strip() == "1"


def test_pkill_query_log(started_node):
    for signal in ["SEGV", "4"]:
        # force create query_log if it was not created
        started_node.query("SYSTEM FLUSH LOGS")
        started_node.query("TRUNCATE TABLE IF EXISTS system.query_log")
        started_node.query("SELECT COUNT(*) FROM system.query_log")
        # logs don't flush
        assert started_node.query("SELECT COUNT(*) FROM system.query_log") == f"{0}\n"

        send_signal(started_node, signal)
        wait_for_clickhouse_stop(started_node)
        started_node.restart_clickhouse()
        assert started_node.query("SELECT COUNT(*) FROM system.query_log") >= "3\n"


REPORT_PREAMBLE = b"CLICKHOUSE SANITIZER REPORT\n"
CORES_DIR = "/var/lib/clickhouse/cores"


def find_report_in_core(core_path):
    # The core is mostly sparse holes: scan only the data extents.
    with open(core_path, "rb") as f, mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ) as core:
        offset = 0
        while True:
            try:
                start = os.lseek(f.fileno(), offset, os.SEEK_DATA)
            except OSError:
                return None
            offset = os.lseek(f.fileno(), start, os.SEEK_HOLE)
            pos = core.find(REPORT_PREAMBLE, start, offset + len(REPORT_PREAMBLE))
            if pos != -1:
                report = core[pos : pos + (1 << 20)]
                terminator = report.find(b"\x00")
                return report[:terminator] if terminator != -1 else report


def test_sanitizer_report_in_core_dump(started_node):
    if not any(
        started_node.is_built_with_sanitizer(name)
        for name in ("address", "thread", "memory")
    ):
        pytest.skip("requires an ASan, TSan or MSan build")

    # Only a server started with --daemon changes its working directory to the
    # cores directory, and the previous test may have restarted it without it.
    started_node.restart_clickhouse(daemon=True)

    cores_dir = os.path.join(started_node.path, "database", "cores")
    for name in os.listdir(cores_dir):
        os.remove(os.path.join(cores_dir, name))

    started_node.query("SYSTEM ENABLE FAILPOINT trigger_sanitizer_error")
    started_node.query("SELECT 1", ignore_error=True)

    wait_for_clickhouse_stop(started_node)

    cores = [os.path.join(cores_dir, name) for name in os.listdir(cores_dir)]
    assert len(cores) == 1

    report = find_report_in_core(cores[0])
    assert report is not None
    assert b"Sanitizer" in report
    assert b"SUMMARY:" in report

    shutil.rmtree(cores_dir, ignore_errors=True)
    started_node.restart_clickhouse()
