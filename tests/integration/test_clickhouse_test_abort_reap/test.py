"""Abort-path reaping of test process groups in `tests/clickhouse-test`.

No cluster: these load the runner as a module and drive the functions that reap the
process groups of tests a stopped run leaves behind. The contracts pinned here:

- a reap only touches the records of its own invocation (`_CLICKHOUSE_TEST_RUN_TOKEN`)
  and of the workers it names;
- `--cleanup`, which names no workers, reaps every record whatever its invocation or
  name shape;
- a `spawn` worker inherits the invocation token, so its records are in the reap scope;
- the reap cannot be interrupted by the signals the runner turns into `Terminated`;
- `quiesce_workers_and_reap` stops the workers before walking the records.
"""

import importlib.machinery
import importlib.util
import json
import multiprocessing
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

RUNNER_PATH = Path(__file__).resolve().parents[2] / "clickhouse-test"
TOKEN_VARIABLE = "_CLICKHOUSE_TEST_RUN_TOKEN"
RUNNER_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)


def load_runner():
    loader = importlib.machinery.SourceFileLoader("clickhouse_test", str(RUNNER_PATH))
    spec = importlib.util.spec_from_loader("clickhouse_test", loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


@pytest.fixture
def runner(tmp_path, monkeypatch):
    # Loading the runner seeds the token into `os.environ`; keep that out of the rest of
    # the session.
    monkeypatch.delenv(TOKEN_VARIABLE, raising=False)
    module = load_runner()
    monkeypatch.setattr(module, "_GROUP_PID_PATH", tmp_path)
    return module


def start_group(seconds=60):
    """A test process group, as `run_single_test` starts one."""
    return subprocess.Popen(["sleep", str(seconds)], start_new_session=True)


def group_is_live(pgid):
    """Whether any process of `pgid` is running. Zombies do not count: an orphan whose
    new parent is slow to reap it has still been killed."""
    for stat in Path("/proc").glob("[0-9]*/stat"):
        try:
            text = stat.read_text()
        except OSError:
            continue
        # `pid (comm) state ppid pgrp ...`, and `comm` may contain spaces.
        fields = text[text.rindex(")") + 2 :].split()
        if int(fields[2]) == pgid and fields[0] not in ("Z", "X"):
            return True
    return False


def wait_until_gone(pgid, timeout=30):
    deadline = time.monotonic() + timeout
    while group_is_live(pgid):
        if time.monotonic() > deadline:
            return False
        time.sleep(0.05)
    return True


def records(directory):
    return sorted(
        p.name for p in Path(directory).iterdir() if not p.name.endswith(".tmp")
    )


def test_reap_is_scoped_to_run_token_and_workers(runner, tmp_path):
    ours = start_group()
    foreign_run = start_group()
    other_worker = start_group()
    try:
        worker = os.getpid()
        runner.write_text_atomic(
            runner.test_process_group_record(ours.pid), f"{ours.pid}\n"
        )
        foreign_record = (
            tmp_path / f"{runner._GROUP_PID_NAME}.1-deadbeef.{worker}.{foreign_run.pid}"
        )
        foreign_record.write_text(f"{foreign_run.pid}\n")
        other_worker_record = (
            tmp_path
            / f"{runner._GROUP_PID_NAME}.{runner._RUN_TOKEN}.{worker + 1}.{other_worker.pid}"
        )
        other_worker_record.write_text(f"{other_worker.pid}\n")

        runner.reap_recorded_test_groups({worker})

        assert ours.wait(timeout=30) is not None
        assert records(tmp_path) == sorted(
            [foreign_record.name, other_worker_record.name]
        )
        # Another invocation's tests, even under a worker pid we name, and our own run's
        # tests under a worker we did not name, are left alone.
        assert foreign_run.poll() is None
        assert other_worker.poll() is None
    finally:
        for proc in (ours, foreign_run, other_worker):
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()


def test_cleanup_reaps_every_record(runner, tmp_path):
    older_runner = start_group()
    killed_run = start_group()
    try:
        worker = os.getpid()
        # What `--cleanup` finds in a reused `ci/tmp`: a record named before the token
        # was added, and one of an invocation that was killed.
        (tmp_path / f"{runner._GROUP_PID_NAME}.{worker}").write_text(
            f"{older_runner.pid}\n"
        )
        (
            tmp_path / f"{runner._GROUP_PID_NAME}.1-deadbeef.{worker}.{killed_run.pid}"
        ).write_text(f"{killed_run.pid}\n")

        runner.cleanup_test_groups()

        assert older_runner.wait(timeout=30) is not None
        assert killed_run.wait(timeout=30) is not None
        assert records(tmp_path) == []
    finally:
        for proc in (older_runner, killed_run):
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()


SPAWN_DRIVER = textwrap.dedent("""
    import importlib.machinery
    import importlib.util
    import json
    import multiprocessing
    import os
    import subprocess
    import sys
    from pathlib import Path

    RUNNER_PATH, RECORD_DIR = sys.argv[1], Path(sys.argv[2])


    def load_runner():
        loader = importlib.machinery.SourceFileLoader("clickhouse_test", RUNNER_PATH)
        spec = importlib.util.spec_from_loader("clickhouse_test", loader)
        module = importlib.util.module_from_spec(spec)
        loader.exec_module(module)
        module._GROUP_PID_PATH = RECORD_DIR
        return module


    def worker(queue):
        runner = load_runner()
        proc = subprocess.Popen(["sleep", "60"], start_new_session=True)
        runner.write_text_atomic(runner.test_process_group_record(proc.pid), f"{proc.pid}\\n")
        queue.put((runner._RUN_TOKEN, os.getpid(), proc.pid))


    if __name__ == "__main__":
        runner = load_runner()
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        process = context.Process(target=worker, args=(queue,))
        process.start()
        worker_token, worker_pid, pgid = queue.get(timeout=60)
        process.join(timeout=60)
        records_before = sorted(p.name for p in RECORD_DIR.iterdir())
        runner.reap_recorded_test_groups({worker_pid})
        records_after = sorted(p.name for p in RECORD_DIR.iterdir())
        print(json.dumps({
            "parent_token": runner._RUN_TOKEN,
            "worker_token": worker_token,
            "pgid": pgid,
            "records_before": records_before,
            "records_after": records_after,
        }))
    """)


def test_run_token_propagates_to_spawn_workers(tmp_path):
    driver = tmp_path / "driver.py"
    driver.write_text(SPAWN_DRIVER)
    record_dir = tmp_path / "records"
    record_dir.mkdir()
    env = {k: v for k, v in os.environ.items() if k != TOKEN_VARIABLE}

    output = subprocess.run(
        [sys.executable, str(driver), str(RUNNER_PATH), str(record_dir)],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout
    result = json.loads(output.strip().splitlines()[-1])
    pgid = result["pgid"]
    try:
        # A `spawn` worker re-executes the runner, which would mint a token of its own
        # and put its records outside the parent's reap scope.
        assert result["worker_token"] == result["parent_token"]
        assert len(result["records_before"]) == 1
        assert f".{result['parent_token']}." in result["records_before"][0]
        assert result["records_after"] == []
        assert wait_until_gone(pgid)
    finally:
        if group_is_live(pgid):
            os.killpg(pgid, signal.SIGKILL)


@pytest.fixture
def runner_signal_handlers(runner):
    previous = [(s, signal.signal(s, runner.signal_handler)) for s in RUNNER_SIGNALS]
    yield
    for s, handler in previous:
        signal.signal(s, handler)


def test_reap_ignores_runner_signals(runner, runner_signal_handlers, monkeypatch):
    kill_process_group = runner.kill_process_group
    signalled = []

    def kill_process_group_under_signals(pgid, fatal_log, diagnostics=True):
        # What a terminated worker's `stop_tests` does to the whole process group, and
        # what CI sends when it stops the job.
        for s in RUNNER_SIGNALS:
            os.kill(os.getpid(), s)
            signalled.append(s)
        kill_process_group(pgid, fatal_log, diagnostics=diagnostics)

    monkeypatch.setattr(runner, "kill_process_group", kill_process_group_under_signals)

    group = start_group()
    try:
        runner.write_text_atomic(
            runner.test_process_group_record(group.pid), f"{group.pid}\n"
        )
        # `Terminated` escaping here would replace the exception the job side keys on.
        try:
            runner.reap_recorded_test_groups({os.getpid()})
        except runner.Terminated as e:
            pytest.fail(f"the reap was interrupted by signal {e.signal}")
        assert signalled == list(RUNNER_SIGNALS)
        assert group.wait(timeout=30) is not None
        for s in RUNNER_SIGNALS:
            assert signal.getsignal(s) is runner.signal_handler
    finally:
        if group.poll() is None:
            os.killpg(group.pid, signal.SIGKILL)
            group.wait()


def test_quiesce_ignores_runner_signals(runner, runner_signal_handlers, monkeypatch):
    signalled = []

    def terminate_workers_under_signals(processes):
        # A terminated worker broadcasts SIGTERM to the group from `stop_tests`.
        for s in RUNNER_SIGNALS:
            os.kill(os.getpid(), s)
            signalled.append(s)

    monkeypatch.setattr(runner, "terminate_workers", terminate_workers_under_signals)

    try:
        runner.quiesce_workers_and_reap([], set())
    except runner.Terminated as e:
        pytest.fail(f"stopping the workers was interrupted by signal {e.signal}")
    assert signalled == list(RUNNER_SIGNALS)
    for s in RUNNER_SIGNALS:
        assert signal.getsignal(s) is runner.signal_handler


def record_groups_forever(record_dir, log_path):
    """A worker that keeps starting tests. SIGTERM only lands between tests, so that
    every group it starts is also recorded and none leaks from the test itself."""
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    runner = load_runner()
    runner._GROUP_PID_PATH = Path(record_dir)
    while True:
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
        proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
        runner.write_text_atomic(
            runner.test_process_group_record(proc.pid), f"{proc.pid}\n"
        )
        with open(log_path, "a") as log:
            log.write(f"{proc.pid}\n")
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM})
        time.sleep(0.005)


def test_quiesce_stops_workers_before_reap(runner, tmp_path, monkeypatch):
    record_dir = tmp_path / "records"
    record_dir.mkdir()
    monkeypatch.setattr(runner, "_GROUP_PID_PATH", record_dir)
    log_path = tmp_path / "started"
    log_path.touch()

    # The workers of `do_run_tests` are `multiprocessing` processes. `fork`, so that
    # the worker shares the parent's run token, as it does in the runner.
    context = multiprocessing.get_context("fork")
    worker = context.Process(
        target=record_groups_forever, args=(str(record_dir), str(log_path))
    )
    worker.start()
    started = []
    try:
        deadline = time.monotonic() + 60
        while len(records(record_dir)) < 5:
            assert worker.is_alive()
            assert time.monotonic() < deadline, "the worker recorded no test groups"
            time.sleep(0.01)

        # The reap walks the records once, so a worker still running would record
        # groups behind it.
        runner.quiesce_workers_and_reap([worker], {worker.pid})

        assert not worker.is_alive()
        assert records(record_dir) == []
        started = [int(line) for line in log_path.read_text().split()]
        assert started
        leaked = [pgid for pgid in started if not wait_until_gone(pgid)]
        assert leaked == []
    finally:
        if worker.is_alive():
            worker.kill()
            worker.join()
        for pgid in started or [int(line) for line in log_path.read_text().split()]:
            if group_is_live(pgid):
                os.killpg(pgid, signal.SIGKILL)
