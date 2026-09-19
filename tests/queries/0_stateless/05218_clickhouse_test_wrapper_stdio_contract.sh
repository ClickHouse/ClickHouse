#!/usr/bin/env bash
# The functional test runner (tests/clickhouse-test) spawns every test through a
# `bash -c '{test} > {stdout} 2> {stderr}'` wrapper in a session of its own. Three
# properties of that hand-over are load-bearing:
#   1. the wrapper must get a stdout that is not the runner's own, because the runner's
#      stdout is the CI pipeline's and a wrapper that outlives the runner keeps it open;
#   2. both per-test output files must be created empty before the wrapper starts, because
#      a wrapper that dies before `exec` never applies its own redirects: the
#      normalization that runs after `wait` reads them unconditionally, and a retried
#      attempt would otherwise report the previous attempt's output;
#   3. what the wrapper writes to the stderr it was given must land after the test's own
#      stderr in the shared per-test file, not on top of it.
# This test loads the runner as a module and drives `run_single_test` with the spawn seam
# replaced, so it needs no server and has no timing dependence.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RUNNER="$CUR_DIR/../../clickhouse-test"
WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05218_clickhouse_test_wrapper_stdio_contract.XXXXXX")
trap 'rm -rf "${WORK_DIR:?}"' EXIT

python3 - "$RUNNER" "$WORK_DIR" <<'PY'
import contextlib
import importlib.machinery
import importlib.util
import io
import os
import subprocess
import sys
import types
from pathlib import Path

runner_path, work_dir = sys.argv[1], sys.argv[2]

loader = importlib.machinery.SourceFileLoader("clickhouse_test_runner", runner_path)
spec = importlib.util.spec_from_loader("clickhouse_test_runner", loader)
runner = importlib.util.module_from_spec(spec)
# Module-level code of the runner may print (e.g. about a missing jinja2). A stateless
# test fails on any stderr output, and none of that is what this test checks.
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    loader.exec_module(runner)

# Keep the runner's group-pid bookkeeping inside this test's own directory.
runner._GROUP_PID_PATH = Path(work_dir)

EARLIER_ATTEMPT = b"Failed to configure cgroup clickhouse-test-1: Permission denied\n"
FATAL_DIAGNOSTIC = b"Failed to configure cgroup clickhouse-test-2: Permission denied\n"
TEST_OWN_STDERR = b"the test writes this to its own stderr\n"
WRAPPER_STDERR = b"Terminated\n"


def shares_runner_stdout(handed):
    """`Popen(stdout=None)` hands the child a copy of the runner's own stdout, and so does
    any descriptor that resolves to the same open file; either keeps the CI pipeline the
    runner heads open for as long as the child lives."""
    if handed is None:
        return True
    fd = handed if isinstance(handed, int) else handed.fileno()
    if fd == subprocess.DEVNULL:
        return False
    if fd < 0:  # PIPE / STDOUT: no independent sink for the child either
        return True
    return os.path.sameopenfile(fd, sys.__stdout__.fileno())


class FakeWrapper:
    """Stands in for the `bash -c` wrapper. Records how the runner handed over the
    standard streams, then replays what the real wrapper does to the per-test files: bash
    opens both redirects with O_TRUNC at exec, and a fatal `preexec_fn` exits the child
    before that, so it can only reach the streams `Popen` installed."""

    case = None
    behaviour = None
    seen = None

    def __init__(self, command, **kwargs):
        self.pid = os.getpid()
        self.returncode = 0
        case = FakeWrapper.case
        handed_stderr = kwargs.get("stderr")
        FakeWrapper.seen = {
            "shares_runner_stdout": shares_runner_stdout(kwargs.get("stdout")),
            "stdout_file_existed": os.path.exists(case.stdout_file),
            "stderr_file_existed": os.path.exists(case.stderr_file),
        }
        writable_stderr = handed_stderr if not isinstance(handed_stderr, int) else None
        if FakeWrapper.behaviour == "dies_before_exec":
            self.returncode = 1  # the fatal preexec_fn branch ends in os._exit(1)
            if writable_stderr is not None:
                writable_stderr.write(FATAL_DIAGNOSTIC)
                writable_stderr.flush()
            return
        open(case.stdout_file, "wb").close()
        with open(case.stderr_file, "wb") as test_own_stderr:
            test_own_stderr.write(TEST_OWN_STDERR)
        if writable_stderr is not None:
            writable_stderr.write(WRAPPER_STDERR)
            writable_stderr.flush()

    def wait(self, timeout=None):
        return 0


def make_case(name):
    case = runner.TestCase.__new__(runner.TestCase)
    case.case_file = os.path.join(work_dir, name + ".sh")
    case.ext = ".sh"
    case.tags = set()
    case.memory_limit = 0
    case.stdout_file = os.path.join(work_dir, name + ".stdout")
    case.stderr_file = os.path.join(work_dir, name + ".stderr")
    case.testcase_args = types.SimpleNamespace(
        bash_tracing_file=os.path.join(work_dir, name + ".xtrace"),
        cloud=False,
        debug_log_file=os.path.join(work_dir, name + ".debuglog"),
        hide_db_name=False,
        memory_limit=0,
        replicated_database=False,
        secure=False,
        shared_catalog=False,
        testcase_basename=name + ".sh",
        testcase_client="clickhouse-client",
        testcase_database="database_" + name,
        testcase_start_time=runner.datetime.now(),
        timeout=600,
        trace=False,
    )
    return case


def spawn(name, behaviour, seed=None):
    """Run one test through the runner's spawn path, optionally with both per-test files
    already holding `seed` as an earlier attempt would leave them. Returns what was
    collected and the failure the runner let escape, if any; anything it printed is
    surfaced on stdout so a reference diff shows it."""
    case = make_case(name)
    if seed is not None:
        for path in (case.stdout_file, case.stderr_file):
            with open(path, "wb") as f:
                f.write(seed)
    FakeWrapper.case = case
    FakeWrapper.behaviour = behaviour
    FakeWrapper.seen = None
    noise = io.StringIO()
    escaped = None
    real_popen = runner.Popen
    runner.Popen = FakeWrapper
    try:
        with contextlib.redirect_stdout(noise), contextlib.redirect_stderr(noise):
            try:
                runner.TestCase.run_single_test(case, "warning", "")
            except Exception as e:  # pylint:disable=broad-except
                escaped = f"{type(e).__name__}: {e}"
    finally:
        runner.Popen = real_popen
    if noise.getvalue():
        print(f"unexpected runner output: {noise.getvalue()!r}")
    collected = {}
    for stream, path in (("stdout", case.stdout_file), ("stderr", case.stderr_file)):
        collected[stream] = b""
        if os.path.exists(path):
            with open(path, "rb") as f:
                collected[stream] = f.read()
    return FakeWrapper.seen, collected, escaped


def report(name, got, expected, detail=None):
    print(f"{name}: {'yes' if got else 'no'} {'OK' if got == expected else 'FAIL'}")
    if got != expected and detail is not None:
        print(f"  {detail}")


# A wrapper killed before `exec` leaves the runner to read files bash never created.
seen, _, escaped = spawn("wrapper_dies_before_exec", "dies_before_exec")
report(
    "per-test files exist before the wrapper starts",
    seen["stdout_file_existed"] and seen["stderr_file_existed"],
    True,
)
report(
    "runner reports such a test instead of failing",
    escaped is None,
    True,
    f"escaped: {escaped}",
)

# The same on a retry, where both files still hold the earlier attempt's output: only the
# wrapper's own diagnostic may survive into what the harness collects and reports.
seen, collected, _ = spawn("wrapper_dies_on_retry", "dies_before_exec", seed=EARLIER_ATTEMPT)
for stream in ("stdout", "stderr"):
    report(
        f"earlier attempt's {stream} is cleared",
        EARLIER_ATTEMPT not in collected[stream],
        True,
        f"collected {stream}: {collected[stream]!r}",
    )
report(
    "wrapper diagnostic reaches the collected stderr",
    FATAL_DIAGNOSTIC in collected["stderr"],
    True,
    f"collected stderr: {collected['stderr']!r}",
)

# A wrapper that execs, writes to both per-test files and then reports a killed job.
seen, collected, _ = spawn("wrapper_writes_and_is_killed", "execs")
report("wrapper stdout is not the runner's own", not seen["shares_runner_stdout"], True)
report(
    "wrapper stderr lands after the test's own",
    collected["stderr"] == TEST_OWN_STDERR + WRAPPER_STDERR,
    True,
    f"collected stderr: {collected['stderr']!r}",
)
PY
