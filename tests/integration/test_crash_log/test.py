import gzip
import mmap
import os
import re
import shlex
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


# The fault handler writes one bare dump under `Stack trace:`, and one under `Job's origin stack
# trace:` for every job that scheduled the faulting one. Longest marker first, so a tail is never
# taken from a partial match.
BARE_TRACE_MARKERS = ("Job's origin stack trace:", "Stack trace:")

# The fault handler logs this once per fault, at fatal level, ahead of both bare dumps, so it is
# what separates one fault's dumps from the next one's in a log that accumulates across restarts.
FAULT_MARKER = "########## Short fault info ############"


def bare_trace_tail(line):
    """The text after a bare dump marker, or None if the line carries no marker."""
    for marker in BARE_TRACE_MARKERS:
        if marker in line:
            return line.split(marker, 1)[1]
    return None


def parse_bare_trace(line):
    """[(address, object_or_None)] from a bare dump line."""
    tokens = bare_trace_tail(line).split()
    entries = []
    i = 0
    while i < len(tokens):
        assert tokens[i].startswith("0x"), f"unparsable bare trace token {tokens[i]!r} in {line!r}"
        if i + 2 < len(tokens) and tokens[i + 1] == "in":
            entries.append((int(tokens[i], 16), tokens[i + 2]))
            i += 3
        else:
            entries.append((int(tokens[i], 16), None))
            i += 1
    return entries


def fatal_trace_blocks(log_text):
    """One entry per bare trace dump: which fault it belongs to, its parsed entries, the addresses
    of the symbolized lines logged after it, and how many of those lines resolved to a symbol. A
    fault logs the bare dump twice, so blocks with no symbolized lines after them are the duplicate
    and carry nothing to compare against. Faults are numbered from one in the order they were
    logged."""
    blocks = []
    fault = 0
    for line in log_text.splitlines():
        if FAULT_MARKER in line:
            fault += 1
        tail = bare_trace_tail(line)
        if tail is not None:
            # Other producers log the same marker followed by a symbolized trace on one line, whose
            # first token is a frame number. A bare dump always starts with an address, so the first
            # token decides; anything else under a marker is not a bare dump and is not a frame line.
            tokens = tail.split()
            if tokens and tokens[0].startswith("0x"):
                blocks.append(
                    {
                        "fault": fault,
                        "entries": parse_bare_trace(line),
                        "symbolized": set(),
                        "resolved": 0,
                    }
                )
            continue
        if not blocks:
            continue
        frame = re.search(r"\d+\.(?:\d+\.)? (.*) @ (0x[0-9a-fA-F]+)$", line)
        if frame:
            blocks[-1]["symbolized"].add(int(frame.group(2), 16))
            if frame.group(1).rsplit(": ", 1)[-1] != "?":
                blocks[-1]["resolved"] += 1
    return blocks


def read_server_log(node):
    """The fault and the restart after it rotate the log, so the trace is usually in a rotated
    sibling and not in the live file. `clickhouse-server.log.0.gz` is the newest rotation, so a
    descending sort of the rotations followed by the live file is chronological, which keeps each
    bare dump next to the symbolized lines that belong to it even across a rotation boundary."""
    names = [n for n in os.listdir(node.logs_dir) if n.startswith("clickhouse-server.log")]
    rotated = sorted((n for n in names if n != "clickhouse-server.log"), reverse=True)
    chunks = []
    for name in rotated + ["clickhouse-server.log"]:
        path = os.path.join(node.logs_dir, name)
        if not os.path.exists(path):
            continue
        opener = gzip.open if name.endswith(".gz") else open
        with opener(path, "rt", errors="replace") as log:
            chunks.append(log.read())
    return "\n".join(chunks)


def test_bare_stack_trace_uses_the_same_addresses_as_the_symbolized_one(started_node):
    started_node.query("SYSTEM ENABLE FAILPOINT sleep_in_logs_flush")
    send_signal(started_node, "SEGV")
    wait_for_clickhouse_stop(started_node)
    started_node.restart_clickhouse()

    log_text = read_server_log(started_node)

    # The log accumulates across restarts and the tests before this one fault the same server, so a
    # check over all of it can be satisfied by an older crash. The fault this test caused is the
    # last one recorded, because the faults of the tests that follow have not happened yet. Counting
    # the faults in the log rather than in the blocks is what makes a fault that logged no bare dump
    # at all fail here instead of quietly handing the checks below the previous fault's trace.
    faults = log_text.count(FAULT_MARKER)
    assert faults, (
        f"the server log carries no {FAULT_MARKER!r} line, so a bare trace cannot be attributed to "
        "the fault that produced it and every check below would run over every crash in the log"
    )
    blocks = [b for b in fatal_trace_blocks(log_text) if b["fault"] == faults and b["symbolized"]]

    assert blocks, "this fault logged no bare stack trace with symbolized lines after it"

    # A frame in the main executable is printed with no object, so the executable itself is what
    # bounds it, and `SymbolIndex` takes that file from `/proc/self/exe`: the running process is the
    # only authority on which file that is.
    main_path = started_node.exec_in_container(
        ["bash", "-c", "readlink -f /proc/$(pgrep -n -x clickhouse)/exe"], nothrow=True
    ).strip()
    assert main_path.startswith("/"), (
        f"cannot read the server executable's path from the container, got {main_path!r}, so the "
        "frames that carry no object would be left with nothing bounding them"
    )
    main_size = started_node.exec_in_container(
        ["bash", "-c", f"stat -c %s {shlex.quote(main_path)}"], nothrow=True
    ).strip()
    assert main_size.isdigit(), (
        f"cannot read the size of {main_path} from the container, got {main_size!r}, so the frames "
        "that carry no object would be left with nothing bounding them"
    )
    main_size = int(main_size)

    for block in blocks:
        # Subtracting the load base is what makes the two comparable, so this is substantive only for
        # a position independent binary; with a fixed load address the base is zero and both sides are
        # the runtime address. Every Linux glibc amd64 and aarch64 build is position independent.
        assert block["resolved"], (
            "no frame resolved to a symbol, so the symbol index is unusable here and the comparison "
            "below cannot tell a normalized address from a runtime one"
        )
        for address, object_name in block["entries"]:
            assert address in block["symbolized"], (
                f"bare trace address {address:#018x} is in a different address space than the "
                "symbolized lines of the same fault"
            )
            if object_name is None:
                # The bound is the size of the whole ELF file, which is larger than the part of it
                # that gets loaded, so it is weaker than the loaded image's own span. It is still
                # orders of magnitude below a runtime address, which is the distinction being made.
                assert address < main_size, (
                    f"bare trace address {address:#018x} is not an offset into {main_path} "
                    f"(its size is {main_size}), so it is still a runtime address"
                )
            elif object_name != "<unknown>":
                # An offset is only interpretable together with its object, so a frame outside the
                # main executable has to name the file that `addr2line` should be pointed at.
                quoted_object = shlex.quote(object_name)
                size = started_node.exec_in_container(
                    ["bash", "-c", f"stat -c %s {quoted_object} 2>/dev/null || echo missing"]
                ).strip()
                assert size != "missing", f"bare trace names {object_name}, which does not exist"
                # `SymbolIndex` bounds each object by the size of the ELF file it was built from, so
                # an offset into that object is always below it, while a runtime address is far above.
                assert address < int(size), (
                    f"bare trace address {address:#018x} is not an offset into {object_name} "
                    f"(its size is {size}), so it is still a runtime address"
                )

    # Collection level, not per block: one fault's trace could in principle be entirely main-object.
    annotated = [(a, o) for b in blocks for a, o in b["entries"] if o is not None and o != "<unknown>"]
    assert annotated, (
        "no bare trace frame was annotated with an object, so the ` in <object>` form that makes "
        "a non-main-executable offset interpretable is not being produced (an <unknown> annotation "
        "does not count: it names no file to point addr2line at)"
    )
    # The unannotated form is the main executable's, and a healthy binary recognizes most frames as
    # its own, so a trace made only of other objects and failed lookups means the classification
    # itself regressed, and it also leaves the executable-size bound above with nothing to check.
    in_main_object = [a for b in blocks for a, o in b["entries"] if o is None]
    assert in_main_object, (
        "no bare trace frame was left unannotated, so not one frame was recognized as being in the "
        "main executable"
    )


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
