"""
Tests for `select_replica_failures` (ci/jobs/stress_job.py) and for the
hung-check `info` cell (ci/jobs/scripts/stress/stress.py).

The stress job pairs each replica's server log with its stderr log and must not
let a failure on one replica hide a (possibly higher-signal) failure on another:
every pair is scanned, all distinct specific classifications are reported, and a
generic `<Fatal>` / "Unknown error" is used only when nothing specific was found.

The `Hung check failed` row embeds a bounded window of `hung_check.log` in the CI
report. `clickhouse-test --hung-check` prints the verdict and the longest-running
queries first, so the window is read from the head: reading the end embedded
whatever trailing output happened to be last, leaving the failure undiagnosable
from CIDB alone.
"""

import ast
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.jobs.scripts.stress.stress import (
    HUNG_CHECK_INFO_BUDGET,
    build_hung_check_info,
    escape_tsv_info,
)
from ci.jobs.stress_job import read_test_results, select_replica_failures

_ORACLE_MISMATCH_LOG = (
    "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> ASTFuzzer: "
    "AST Fuzzer oracle mismatch detected!\n"
    "Fuzzed query: SELECT g FROM t GROUP BY g\n"
    "TLP Aggregate oracle mismatch!\n"
    "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n"
)
_ASAN_STDERR = (
    "==1234==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
    "    #0 0x55b9b8db9bc7 in DB::Foo::bar() src/Foo.cpp:10:5\n"
    "SUMMARY: AddressSanitizer: heap-use-after-free src/Foo.cpp:10:5\n"
)
_GENERIC_FATAL_LOG = (
    "2026.09.04 00:44:57.972626 [ 1068 ] {q} <Fatal> SomeComponent: "
    "transient generic fatal\n"
    "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n"
)
_UNKNOWN_LOG = (
    "2026.09.04 00:44:57.900000 [ 1068 ] {q} <Debug> executeQuery: SELECT 1\n"
    "2026.09.04 00:44:58.000000 [ 1068 ] {} <Information> Application: shutting down\n"
)


def _pair(tmp_path, replica, server_text=None, stderr_text=None):
    # Build one (replica_name, server_log, stderr_log) pair. Missing logs are
    # non-existent Paths, matching what the job passes when a file is absent.
    server_log = tmp_path / f"clickhouse-server-{replica}.err.log"
    stderr_log = tmp_path / f"stderr-{replica}.log"
    if server_text is not None:
        server_log.write_text(server_text, encoding="utf-8")
    if stderr_text is not None:
        stderr_log.write_text(stderr_text, encoding="utf-8")
    return (replica, server_log, stderr_log)


def test_specific_failure_on_second_replica_not_hidden_by_first(tmp_path):
    # Main replica has an oracle mismatch, sc1 has an ASan report. Both are
    # specific classifications and must both be reported - crucially the ASan
    # report on the second replica is not suppressed by the first.
    pairs = [
        _pair(tmp_path, "main", server_text=_ORACLE_MISMATCH_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG, stderr_text=_ASAN_STDERR),
    ]

    results = select_replica_failures(pairs)
    names = [name for name, _, _ in results]

    assert any(n.startswith("AST Fuzzer oracle mismatch") for n in names), names
    assert any(n.startswith("AddressSanitizer") for n in names), names
    assert len(results) == 2


def test_generic_fatal_does_not_suppress_sanitizer_on_second_replica(tmp_path):
    # Main replica has only a generic <Fatal>, sc1 has an ASan report. The
    # specific sanitizer failure wins; the generic fatal is dropped.
    pairs = [
        _pair(tmp_path, "main", server_text=_GENERIC_FATAL_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG, stderr_text=_ASAN_STDERR),
    ]

    results = select_replica_failures(pairs)
    names = [name for name, _, _ in results]

    assert len(results) == 1
    assert names[0].startswith("AddressSanitizer")
    assert "transient generic fatal" not in names[0]


def test_same_specific_failure_across_replicas_reported_once(tmp_path):
    # Replicated setups surface the same failure on several replicas; report a
    # given specific classification only once.
    pairs = [
        _pair(tmp_path, "main", server_text=_ORACLE_MISMATCH_LOG),
        _pair(tmp_path, "sc1", server_text=_ORACLE_MISMATCH_LOG),
    ]

    results = select_replica_failures(pairs)

    assert len(results) == 1
    assert results[0][0].startswith("AST Fuzzer oracle mismatch")


def test_generic_fatal_used_when_no_specific_failure(tmp_path):
    # No replica has a specific classification: the generic <Fatal> is reported
    # (once) rather than "Unknown error".
    pairs = [
        _pair(tmp_path, "main", server_text=_GENERIC_FATAL_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG),
    ]

    results = select_replica_failures(pairs)

    assert len(results) == 1
    assert results[0][0] == "SomeComponent: transient generic fatal"


def test_unknown_error_when_nothing_classified(tmp_path):
    # Neither specific nor generic fatal anywhere: a single "Unknown error".
    pairs = [
        _pair(tmp_path, "main", server_text=_UNKNOWN_LOG),
        _pair(tmp_path, "sc1", server_text=_UNKNOWN_LOG),
    ]

    results = select_replica_failures(pairs)

    assert len(results) == 1
    assert results[0][0] == FuzzerLogParser.UNKNOWN_ERROR


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "test_results.tsv"
    path.write_text(content, encoding="utf-8")
    return path


_VERDICT = "Found hung queries in processlist:"


def _hung_log(tmp_path: Path, content) -> Path:
    path = tmp_path / "hung_check.log"
    path.write_bytes(content if isinstance(content, bytes) else content.encode("utf-8"))
    return path


def _processlist(count: int) -> str:
    # `ORDER BY elapsed DESC`: query 0 is the longest-running one.
    return "".join(
        f"query:   SELECT hung_query_{i} FROM t\nelapsed: {count - i}.0\n"
        for i in range(count)
    )


def test_verdict_survives_a_large_log_statistics_section(tmp_path):
    """Regression: `--report-logs-stats` output is unbounded in bytes and printed
    last, so reading the end of the log embedded statistics and never the verdict.
    On the real 309 KB artifact that section is 9.4x the whole budget."""
    log = _hung_log(
        tmp_path,
        "banner\n" * 100
        + f"{_VERDICT}\n"
        + _processlist(1200)
        + "Top patterns of log messages:\n"
        + "count message_format_string\n" * 2000,
    )
    info = build_hung_check_info(log)
    assert _VERDICT in info
    assert "hung_query_0 " in info
    assert "message_format_string" not in info


def test_oldest_query_is_kept_when_the_processlist_exceeds_the_budget(tmp_path):
    """The discriminating case: a genuine processlist larger than the budget, with
    no statistics section at all. Reading the end loses the verdict here too."""
    log = _hung_log(tmp_path, f"banner\n{_VERDICT}\n" + _processlist(1200))
    info = build_hung_check_info(log)
    assert _VERDICT in info
    assert "hung_query_0 " in info
    assert "hung_query_1199 " not in info
    assert "showing the first 32 KiB" in info


def test_a_single_query_line_longer_than_the_budget_is_still_shown(tmp_path):
    """`system.processes.query` is unbounded and `Vertical` does not escape it, so
    one fuzzer query is one line longer than the budget. The window must keep its
    trailing fragment; trimming to a line boundary would erase the processlist."""
    huge = "SELECT hung_query_0, " + "x" * (2 * HUNG_CHECK_INFO_BUDGET)
    log = _hung_log(tmp_path, f"banner\n{_VERDICT}\nquery:   {huge}\n")
    info = build_hung_check_info(log)
    assert "hung_query_0" in info
    assert len(info) > HUNG_CHECK_INFO_BUDGET


def test_hung_check_polling_loop_is_throttled():
    """The loop prints one progress token per probe *before* the verdict, so an
    unthrottled loop can fill the embedded window in the genuine-hang case.

    "Some sleep exists somewhere under the loop" is not the contract: `sleep(0)`,
    a sub-second interval, and a `sleep(1)` nested in the `hung_count == 0` branch
    all leave the loop unthrottled on the path that matters. Pin the pace (a
    literal of at least one second per probe), the depth (a direct body statement)
    and the position (after the break).
    """
    source = Path(__file__).resolve().parents[2] / "tests" / "clickhouse-test"
    tree = ast.parse(source.read_text(encoding="utf-8"))

    def calls(node, name):
        return [
            n
            for n in ast.walk(node)
            if isinstance(n, ast.Call) and getattr(n.func, "id", None) == name
        ]

    loops = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.While) and calls(n, "get_processlist_size")
    ]
    assert len(loops) == 1, f"expected one hung-check polling loop, found {len(loops)}"
    loop = loops[0]

    # `loop.body`, not `ast.walk`: a sleep nested inside a branch does not pace
    # the loop, and `ast.walk` cannot tell the two placements apart.
    sleeps = [
        (i, stmt)
        for i, stmt in enumerate(loop.body)
        if isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Call)
        and getattr(stmt.value.func, "id", None) == "sleep"
    ]
    assert len(sleeps) == 1, (
        "the hung-check polling loop must pace its probes with exactly one sleep"
        f" among its own body statements, found {len(sleeps)}"
    )
    index, sleep_stmt = sleeps[0]

    interval = sleep_stmt.value.args
    assert len(interval) == 1 and isinstance(
        interval[0], ast.Constant
    ), "the polling loop's sleep interval must be a literal so it is reviewable"
    assert isinstance(interval[0].value, (int, float)) and interval[0].value >= 1, (
        "the polling loop must sleep at least one second per probe: the 90-second"
        " deadline then yields ~90 probes and ~180 bytes of progress tokens, which"
        " fits the embedded window. sleep(0) throttles nothing and a sub-second"
        f" interval floods it (found {interval[0].value!r})"
    )

    breaks = [
        i
        for i, stmt in enumerate(loop.body)
        if any(isinstance(n, ast.Break) for n in ast.walk(stmt))
    ]
    assert breaks, "expected the zero-hung fast path to break out of the loop"
    assert index > max(breaks), (
        "the sleep must follow the zero-hung break, so that it paces the probes"
        " on the path taken when queries are hung and a clean run never sleeps"
    )


def test_call_site_embeds_the_head_window_into_test_results():
    """The hung-check call site must keep routing `info` through `build_hung_check_info`.

    Every other test here calls the helper directly, so reverting the call site
    alone would reinstate the tail read with the whole suite green. Extracting
    the logic into a helper is what opened that gap: it used to be inline.

    Shape is not enough - the data flow is pinned too: the helper is handed the
    log, its result is the last write to `info_field`, and the row interpolates
    that name.

    The call site is located by the row it writes, not by function name, so that
    moving the hung-check block between functions cannot silently unpin it.
    """
    source = (
        Path(__file__).resolve().parents[2]
        / "ci"
        / "jobs"
        / "scripts"
        / "stress"
        / "stress.py"
    )
    tree = ast.parse(source.read_text(encoding="utf-8"))
    owners = [
        n
        for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and [
            a
            for a in ast.walk(n)
            if isinstance(a, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "hung_check_status"
                for t in a.targets
            )
        ]
    ]
    assert len(owners) == 1, (
        "expected exactly one function in stress.py to assign hung_check_status,"
        f" found {len(owners)}: {[n.name for n in owners]}"
    )
    call_site = owners[0]

    builders = [
        n
        for n in ast.walk(call_site)
        if isinstance(n, ast.Call)
        and getattr(n.func, "id", None) == "build_hung_check_info"
    ]
    assert len(builders) == 1, (
        "the call site must build the hung-check info cell through"
        " build_hung_check_info"
        f" exactly once, found {len(builders)}"
    )
    builder = builders[0]
    assert (
        len(builder.args) == 1
        and isinstance(builder.args[0], ast.Name)
        and builder.args[0].id == "hung_check_log"
    ), "build_hung_check_info must be given the hung-check log, not another file"

    # The NESTING, not mere co-occurrence: a second, unescaped path would still
    # satisfy "both names appear somewhere in the function".
    wired = [
        n
        for n in ast.walk(call_site)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "info_field" for t in n.targets)
        and isinstance(n.value, ast.Call)
        and getattr(n.value.func, "id", None) == "escape_tsv_info"
        and len(n.value.args) == 1
        and isinstance(n.value.args[0], ast.Call)
        and getattr(n.value.args[0].func, "id", None) == "build_hung_check_info"
    ]
    assert len(wired) == 1, (
        "expected `info_field = escape_tsv_info(build_hung_check_info(...))` in"
        f" the call site, found {len(wired)} such assignments"
    )

    # ORDERING, which the assertion above cannot see: the call site legitimately
    # pre-initialises `info_field = ""` for the OSError path, so the nested form
    # existing is not enough - it must also be the write that wins.
    assigns = sorted(
        (
            n
            for n in ast.walk(call_site)
            if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "info_field" for t in n.targets)
        ),
        key=lambda n: (n.lineno, n.col_offset),
    )
    assert assigns, "the call site must assign info_field"
    last = assigns[-1]
    assert (
        isinstance(last.value, ast.Call)
        and getattr(last.value.func, "id", None) == "escape_tsv_info"
        and len(last.value.args) == 1
        and isinstance(last.value.args[0], ast.Call)
        and getattr(last.value.args[0].func, "id", None) == "build_hung_check_info"
    ), (
        "the LAST write to info_field must be escape_tsv_info(build_hung_check_info(...));"
        " a later assignment silently discards the head window"
    )

    # And the cell has to reach the row: a window computed and never interpolated
    # is the pre-#103551 empty field with extra steps.
    rows = [
        n
        for n in ast.walk(call_site)
        if isinstance(n, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "hung_check_status" for t in n.targets
        )
    ]
    assert (
        len(rows) == 1
    ), f"expected one hung_check_status assignment, found {len(rows)}"
    interpolated = {
        f.value.id
        for f in ast.walk(rows[0].value)
        if isinstance(f, ast.FormattedValue) and isinstance(f.value, ast.Name)
    }
    assert "info_field" in interpolated, (
        "the Hung check failed row must interpolate info_field, or the head window is"
        " computed and thrown away"
    )

    assert not [
        n
        for n in ast.walk(call_site)
        if isinstance(n, ast.Attribute) and n.attr == "SEEK_END"
    ], "the call site must not seek to the end of the log; reading the head is the fix"
    assert not [
        n
        for n in ast.walk(call_site)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "seek"
    ], "the call site must not seek in the log; build_hung_check_info owns the read"


def test_small_log_is_embedded_whole_and_round_trips(tmp_path):
    raw = b"banner\nNo queries hung.\nnul\0tab\tcr\rlf\ninvalid utf-8: \xff\n"
    log = _hung_log(tmp_path, raw)
    info = build_hung_check_info(log)
    assert info == raw.decode("utf-8", errors="replace")
    assert "truncated" not in info
    path = _write(tmp_path, f"row\tFAIL\t\\N\t{escape_tsv_info(info)}\n")
    results, malformed = read_test_results(path)
    assert malformed == []
    # `read_test_results` restores tab/CR/LF; NUL stays escaped as `\0`.
    assert [r.info for r in results] == [info.replace("\0", "\\0")]
