#!/usr/bin/env bash
# The functional test runner (tests/clickhouse-test) kills the process group of a test that exceeded its
# timeout and reports it as `Timeout!`. A test whose process exits right at the deadline is not reaped yet at
# that point, and macOS hides such a process: `getpgid` fails with ESRCH, and `killpg` of a group with no
# running member fails with EPERM. This test loads the runner as a module, answers both calls that way, and
# checks that such a test is still reported as a timeout instead of failing with an internal error of the
# runner, and which signals the runner sent, when the last running member exits before SIGTSTP (sent first
# with `--capture-client-stacktrace`), before SIGTERM, or between SIGTERM and SIGKILL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RUNNER="$CUR_DIR/../../clickhouse-test"

python3 - "$RUNNER" "$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME" <<'PY'
import contextlib
import errno
import importlib.machinery
import importlib.util
import io
import os
import signal
import subprocess
import sys
import types

runner_path, prefix = sys.argv[1], sys.argv[2]
loader = importlib.machinery.SourceFileLoader("clickhouse_test_runner", runner_path)
spec = importlib.util.spec_from_loader("clickhouse_test_runner", loader)
runner = importlib.util.module_from_spec(spec)
# Module-level code of the runner may print warnings; they are not part of what this test checks.
with contextlib.redirect_stdout(io.StringIO()):
    loader.exec_module(runner)

group = None
failing = None
members_exited = False
sent = []


def getpgid(pid):
    assert pid == group, pid
    raise ProcessLookupError(errno.ESRCH, os.strerror(errno.ESRCH))


def killpg(pgid, sig):
    global members_exited
    assert pgid == group, pgid
    if sig != 0:
        sent.append(signal.Signals(sig).name)
    # The last running member of the group exits just before `failing` is sent.
    members_exited = members_exited or sig == failing
    if members_exited:
        raise PermissionError(errno.EPERM, os.strerror(errno.EPERM))


os.getpgid = getpgid
os.killpg = killpg

for capture, failing in (
    (False, signal.SIGTERM),
    (False, signal.SIGKILL),
    (True, signal.SIGTSTP),
):
    runner.CAPTURE_CLIENT_STACKTRACE = capture
    members_exited = False
    sent.clear()
    # Nothing has reaped it, so `returncode` is unset, as for a test whose deadline has passed.
    proc = subprocess.Popen(["true"], start_new_session=True)
    group = proc.pid
    test = types.SimpleNamespace(
        args=None,
        name="timed_out",
        testcase_args=types.SimpleNamespace(
            debug_log_file=f"{prefix}.debuglog", bash_tracing_file=f"{prefix}.trace"
        ),
        stdout_file=f"{prefix}.stdout",
        stderr_file=f"{prefix}.stderr",
        fatal_sanitizer_prefix=f"{prefix}.fatal",
    )
    try:
        result = runner.TestCase.process_result_impl(test, proc, 60.0)
        print(
            f"capture_client_stacktrace={capture}, last member exits before {failing.name}:"
            f" {result.status.value} {result.reason.value} signals sent: {' '.join(sent)}"
        )
    finally:
        proc.wait()
PY
