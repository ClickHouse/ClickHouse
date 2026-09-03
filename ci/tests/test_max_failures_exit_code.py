"""
End-to-end test for the `--max-failures` / `--max-failures-chain` exit code in
parallel runs of `tests/clickhouse-test`.

When a parallel worker reaches the failure limit it raises `StopTesting` inside
its own process. That exit code only reaches the launcher if the parent
survives the worker's shutdown and re-raises with `MAX_FAILURES_EXIT_CODE`.
A worker that broadcast SIGTERM to the whole group (via `stop_tests`) would
kill the parent first, so the run would exit 143 and the job side would
misreport it as "Server died". This test pins the exit code to
`MAX_FAILURES_EXIT_CODE` for both the chain-limit and total-limit paths.

Needs a running ClickHouse server (provided by the CI Tests job).
"""

import runpy
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")

_ct = runpy.run_path(_CLICKHOUSE_TEST)
MAX_FAILURES_EXIT_CODE = _ct["MAX_FAILURES_EXIT_CODE"]
STOP_TESTING_EXIT_CODE = _ct["STOP_TESTING_EXIT_CODE"]


def _make_failing_suite(queries_dir: Path, count: int):
    """Create `count` always-failing parallel `.sh` tests (stdout never matches
    the reference) under `<queries_dir>/0_stateless`."""
    suite = queries_dir / "0_stateless"
    suite.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        name = f"{i:05d}_always_fail"
        script = suite / f"{name}.sh"
        script.write_text("#!/usr/bin/env bash\necho fail\n", encoding="utf-8")
        script.chmod(0o755)
        (suite / f"{name}.reference").write_text("ok\n", encoding="utf-8")


def _run(queries_dir: Path, extra_args):
    proc = subprocess.run(
        [
            sys.executable,
            _CLICKHOUSE_TEST,
            "--queries",
            str(queries_dir),
            "--no-stateful",
            "-j",
            "2",
            *extra_args,
            "00",  # name filter: matches all the fixtures above
        ],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=120,
    )
    return proc


def test_parallel_chain_limit_exits_with_max_failures_code(tmp_path):
    """`--max-failures-chain` only (`--max-failures 0`) - the path the old code
    misreported, because the parent never re-raised with the right code."""
    _make_failing_suite(tmp_path, count=4)
    proc = _run(tmp_path, ["--max-failures", "0", "--max-failures-chain", "1"])
    assert proc.returncode == MAX_FAILURES_EXIT_CODE, (
        f"expected {MAX_FAILURES_EXIT_CODE}, got {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert proc.returncode != STOP_TESTING_EXIT_CODE
    assert proc.returncode != 128 + 15  # not SIGTERM-killed (143 -> "Server died")


def test_parallel_total_limit_exits_with_max_failures_code(tmp_path):
    """`--max-failures` total limit, chain effectively disabled."""
    _make_failing_suite(tmp_path, count=4)
    proc = _run(tmp_path, ["--max-failures", "2", "--max-failures-chain", "1000"])
    assert proc.returncode == MAX_FAILURES_EXIT_CODE, (
        f"expected {MAX_FAILURES_EXIT_CODE}, got {proc.returncode}\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
