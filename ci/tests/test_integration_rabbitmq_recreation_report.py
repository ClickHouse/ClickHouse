"""
Tests for `report_rabbitmq_recreations`, the job-end consumer of the RabbitMQ
container-recreation token in `ci.jobs.integration_test_job`.

The integration test helper recreates a RabbitMQ container that hangs on startup and
retries, which turns a whole aborted test module into a passing one. That recovery is
otherwise invisible: the job is green and nothing records that the infrastructure
misbehaved. The waiter therefore emits a token line into the per-worker pytest log and
preserves the broker log, and this reporter scans the logs at job end to publish the
count and attach the preserved logs.

The waiter side is pinned by `tests/integration/test_cluster_waiters/`. This module
pins the consumer, whose contract is easy to break silently because every failure mode
leaves the job green: a miscount, a lost attachment, a status it must not touch, or -
the reason the reporter runs where it does - the job summary it must not displace.

`Result._add_job_summary_to_info` writes `Failures: N/M` only while `info` is empty, and
`complete_job` calls it after this reporter, so writing `info` unconditionally here would
delete the failure count from a red job's report and from its CIDB row.

See ClickHouse/ClickHouse#114434 (review).
"""

import ast
import json
import os
import sys
import tracemalloc
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.integration_test_job as job
from ci.praktika.result import Result

TOKEN = job.RABBITMQ_RECREATE_TOKEN
REPORTER = "report_rabbitmq_recreations"
CLEANER = "clear_rabbitmq_recreation_scan_inputs"
BATCH = "run_pytest_and_collect_results"


def _token_line(attempt=1, snapshot="", call=1):
    """Render a token line the way the waiter's `logging.warning` call does.

    Built from the shipped format rather than hand-written, so a reword of the waiter's
    message that keeps the token still exercises the real parsing.
    """
    body = (
        "%s attempt=%s snapshot=%s RabbitMQ did not start in %s seconds,"
        " recreating the container" % (TOKEN, attempt, snapshot, 120)
    )
    return (
        f"2026-08-15 06:30:00.000000 [ 1990 ] Warning : {body} (cluster.py, call{call})"
    )


@pytest.fixture(name="temp_path")
def _temp_path(tmp_path, monkeypatch):
    """Point the reporter's module-level `temp_path` at a private directory."""
    monkeypatch.setattr(job, "temp_path", str(tmp_path))
    return tmp_path


def _write_log(temp_path, name, lines):
    (temp_path / name).write_text("".join(line + "\n" for line in lines))


def _snapshot(temp_path, name):
    """Create a preserved broker log where the waiter writes it, and return the field
    value the waiter logs for it.

    That value is the bare name, not a path: the directory holding it may contain
    whitespace, which the whitespace-delimited field below could not carry.
    """
    (temp_path / name).write_text("fake rabbit.log body\n")
    return name


def _attached(temp_path, name):
    """The absolute path the reporter must attach for a snapshot named `name`."""
    return os.path.join(str(temp_path), name)


def _result(status=Result.Status.OK, children=()):
    return Result(
        name="Integration tests (stub)",
        status=status,
        results=[
            Result(name=f"test_{i}", status=child) for i, child in enumerate(children)
        ],
    )


# --- no events ------------------------------------------------------------------------


def test_no_token_reports_nothing(temp_path):
    """A job with no recreation must be left byte-for-byte alone, including `info`:
    `complete_job` still needs it empty to add the failure summary."""
    _write_log(temp_path, "pytest_parallel-gw0.log", ["noise", "RabbitMQ is available"])
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 0
    assert result.info == ""
    assert result.files == []
    assert result.status == Result.Status.OK


def test_no_logs_at_all_reports_nothing(temp_path):
    """An empty `ci/tmp` must not raise: the reporter runs on every job, including ones
    whose pytest never started."""
    assert job.report_rabbitmq_recreations(_result(children=[Result.Status.OK])) == 0


# --- counting -------------------------------------------------------------------------


def test_counts_tokens_across_worker_logs(temp_path):
    """Every per-worker log is scanned, and each token counted exactly once."""
    _write_log(temp_path, "pytest_parallel.log", ["master log, no token"])
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=_snapshot(temp_path, "gw0-attempt1.log"))],
    )
    _write_log(
        temp_path,
        "pytest_parallel-gw1.log",
        [_token_line(snapshot=_snapshot(temp_path, "gw1-attempt1.log"))],
    )
    _write_log(
        temp_path,
        "pytest_sequential.log",
        [_token_line(snapshot=_snapshot(temp_path, "seq-attempt1.log"))],
    )
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 3
    assert "recreation was attempted 3 time(s)" in result.info
    assert len(result.files) == 3


def test_repeated_lines_in_one_appended_log_are_distinct_events(temp_path):
    """A sequential batch appends to one log across repeats, so N token lines in one
    file are N real recreations - not one event seen N times."""
    _write_log(
        temp_path,
        "pytest_sequential.log",
        [
            _token_line(snapshot=_snapshot(temp_path, f"proj{i}-attempt1.log"))
            for i in "ABC"
        ],
    )
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 3
    assert len(result.files) == 3


def test_non_pytest_logs_are_not_scanned(temp_path):
    """Only the pytest logs are the waiter's output channel; a token echoed into some
    other file in `ci/tmp` must not inflate the count."""
    _write_log(temp_path, "other.log", [_token_line()])
    assert job.report_rabbitmq_recreations(_result(children=[Result.Status.OK])) == 0


def test_scan_peak_memory_does_not_grow_with_log_size(temp_path):
    """The scan runs on every integration job, and a per-worker log is tens of MB, so
    peak memory must be bounded by the longest line rather than by the file.

    A token sits on either side of the filler: a scan that buys its memory bound by
    reading only the tail would still see the last one, and must not pass here."""
    first = _snapshot(temp_path, "gw0-attempt1.log")
    last = _snapshot(temp_path, "gw0-attempt2.log")
    filler = "x" * 4096
    body = "".join(f"{filler} line {i}\n" for i in range(6000))
    big = temp_path / "pytest_parallel-gw0.log"
    big.write_text(
        _token_line(attempt=1, snapshot=first)
        + "\n"
        + body
        + _token_line(attempt=2, snapshot=last)
        + "\n"
    )
    size = big.stat().st_size
    assert size > 8 * 1024 * 1024, size
    result = _result(children=[Result.Status.OK])

    tracemalloc.start()
    try:
        tracemalloc.reset_peak()
        assert job.report_rabbitmq_recreations(result) == 2
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result.files == [_attached(temp_path, first), _attached(temp_path, last)]
    assert peak < size // 8, f"peak {peak} against a {size}-byte log"


# --- attaching the preserved broker logs ----------------------------------------------


def test_missing_snapshot_is_counted_but_not_attached(temp_path):
    """The count is the event record; the attachment is best effort. A path that no
    longer exists must not be attached, and must not suppress the count."""
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot="rabbit-proj-gw0-pid1-call1-attempt9.log")],
    )
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 1
    assert result.files == []


def test_empty_snapshot_is_counted_but_not_attached(temp_path):
    """The waiter logs an empty `snapshot=` when it could not copy the broker log."""
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot="")])
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 1
    assert result.files == []


def test_snapshot_is_attached_when_the_scan_directory_contains_spaces(
    tmp_path, monkeypatch
):
    """A checkout can sit under a path with a space (`/Users/alice/ClickHouse Work`).
    The field is whitespace-delimited, so it carries a bare name and the directory is
    supplied here; a path in the field would be cut at the space and the log lost."""
    spacey = tmp_path / "ClickHouse Work" / "ci" / "tmp"
    spacey.mkdir(parents=True)
    monkeypatch.setattr(job, "temp_path", str(spacey))
    name = _snapshot(spacey, "rabbit-proj-gw0-pid1-call1-attempt1.log")
    _write_log(spacey, "pytest_parallel-gw0.log", [_token_line(snapshot=name)])
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 1
    assert result.files == [str(spacey / name)]


@pytest.mark.parametrize(
    "field",
    ["../outside.log", "sub/inside.log", "/etc/passwd", ".", "a b.log"],
    ids=["parent", "subdir", "absolute", "dot", "space"],
)
def test_only_a_bare_name_in_the_scan_directory_is_attached(temp_path, field):
    """The waiter logs a name, so anything else in the field is a truncated or forged
    value rather than a preserved log. It is still counted - the recreation happened -
    but attaching it would upload a file the waiter never wrote, and `.` names a
    directory, which praktika reports as a missing file."""
    (temp_path / "outside.log").write_text("not ours\n")
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=field)])
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 1
    assert result.files == []


def test_duplicate_snapshot_path_is_attached_once(temp_path):
    """Two tokens naming one file are two events but one attachment: praktika uploads
    `files` as-is, so a repeat would be uploaded twice."""
    path = _snapshot(temp_path, "same-attempt1.log")
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=path), _token_line(snapshot=path)],
    )
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 2
    assert result.files == [_attached(temp_path, path)]


def test_preexisting_attachments_are_kept(temp_path):
    """Other collectors attach before this runs."""
    path = _snapshot(temp_path, "attempt1.log")
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=path)])
    result = _result(children=[Result.Status.OK])
    result.files.append("/some/earlier/artifact.tar.gz")

    job.report_rabbitmq_recreations(result)
    assert result.files == [
        "/some/earlier/artifact.tar.gz",
        _attached(temp_path, path),
    ]


def test_unreadable_log_is_skipped_and_others_still_counted(temp_path):
    """One unreadable log must not lose the whole report."""
    bad = temp_path / "pytest_parallel-gw0.log"
    bad.mkdir()  # a directory raises OSError on open
    _write_log(
        temp_path,
        "pytest_parallel-gw1.log",
        [_token_line(snapshot=_snapshot(temp_path, "gw1-attempt1.log"))],
    )
    result = _result(children=[Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 1
    assert len(result.files) == 1


def test_log_that_fails_mid_read_contributes_nothing(temp_path, monkeypatch):
    """A file is scanned line by line, so a read that dies part way through has already
    seen some of its tokens. Publishing those would report an undercount of that file as
    if it were the whole truth, so the file must contribute nothing at all."""
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=_snapshot(temp_path, "gw0-attempt1.log"))],
    )
    doomed = temp_path / "pytest_parallel-gw1.log"
    doomed_lines = [
        _token_line(attempt=n, snapshot=_snapshot(temp_path, f"gw1-attempt{n}.log"))
        for n in (1, 2)
    ]
    _write_log(temp_path, doomed.name, doomed_lines)

    real_open = Path.open

    class _DiesAfterFirstLine:
        """A handle that yields one token line, then fails like a read error would."""

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            return False

        def __iter__(self):
            yield doomed_lines[0] + "\n"
            raise OSError("simulated read error mid-file")

    def _open(self, *args, **kwargs):
        if self.name == doomed.name:
            return _DiesAfterFirstLine()
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", _open)
    result = _result(children=[Result.Status.OK])

    # Only the intact log counts, and the doomed log's snapshot is not attached.
    assert job.report_rabbitmq_recreations(result) == 1
    assert result.files == [
        _attached(temp_path, _snapshot(temp_path, "gw0-attempt1.log"))
    ]


# --- the report must not displace the job summary, or claim recovery ------------------


def test_red_job_keeps_its_failure_count(temp_path):
    """`complete_job` adds `Failures: N/M` only while `info` is empty and runs after
    this, so the reporter must emit the summary itself or the count is lost."""
    snapshot = _snapshot(temp_path, "attempt1.log")
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=snapshot)])
    result = _result(
        status=Result.Status.FAIL,
        children=[Result.Status.OK, Result.Status.FAIL, Result.Status.OK],
    )

    assert job.report_rabbitmq_recreations(result) == 1
    assert "Failures: 1/3" in result.info
    assert "recreation was attempted 1 time(s)" in result.info
    assert result.files == [_attached(temp_path, snapshot)]


def test_green_job_keeps_its_failure_count(temp_path):
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=_snapshot(temp_path, "attempt1.log"))],
    )
    result = _result(children=[Result.Status.OK, Result.Status.OK])

    assert job.report_rabbitmq_recreations(result) == 1
    assert "Failures: 0/2" in result.info
    assert "recreation was attempted 1 time(s)" in result.info


def test_summary_is_not_duplicated_by_complete_job(temp_path):
    """`complete_job` calls `_add_job_summary_to_info` after this reporter. Writing the
    summary here must not produce two of them when that call lands."""
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=_snapshot(temp_path, "attempt1.log"))],
    )
    result = _result(
        status=Result.Status.FAIL,
        children=[Result.Status.OK, Result.Status.FAIL],
    )

    job.report_rabbitmq_recreations(result)
    result._add_job_summary_to_info()  # what complete_job does next
    assert result.info.count("Failures:") == 1, result.info
    assert "Failures: 1/2" in result.info
    assert result.info.count("recreation was attempted") == 1, result.info


def test_report_does_not_claim_recovery(temp_path, capsys):
    """The waiter logs the token before it removes the old container, brings the new one
    up and re-resolves its ID, and every one of those can raise; the retry can also be
    exhausted with the module still aborting. So the token means an attempt was started,
    not that a container was recreated or that the suite recovered, and the wording must
    hold on every path and on both channels the reporter writes."""
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=_snapshot(temp_path, "attempt1.log"))],
    )
    result = _result(status=Result.Status.FAIL, children=[Result.Status.FAIL])

    job.report_rabbitmq_recreations(result)
    stdout = capsys.readouterr().out
    for channel in (result.info, stdout):
        lowered = channel.lower()
        assert "recover" not in lowered, channel
        assert "absorb" not in lowered, channel
        assert "was recreated" not in lowered, channel
    assert "recreation was attempted 1 time(s)" in result.info
    assert "observed: 1" in stdout, stdout


def test_existing_info_is_preserved_and_not_double_summarized(temp_path):
    """An earlier writer's `info` is what suppresses `complete_job`'s summary, so the
    reporter must append to it rather than add a second summary of its own."""
    snapshot = _snapshot(temp_path, "attempt1.log")
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=snapshot)])
    result = _result(status=Result.Status.ERROR, children=[Result.Status.FAIL])
    result.set_info("Session-level error from another writer")

    job.report_rabbitmq_recreations(result)
    assert "Session-level error from another writer" in result.info
    assert "recreation was attempted 1 time(s)" in result.info
    assert "Failures:" not in result.info
    assert result.files == [_attached(temp_path, snapshot)]


# --- the report is not a verdict ------------------------------------------------------


def test_status_and_labels_are_untouched(temp_path):
    """A recreation is not itself a verdict: the reporter only appends to `info` and
    `files`. In particular no INFRA label is added, and the preserved broker log is
    attached whatever the outcome was - a red job is when it is most wanted."""
    snapshot = _snapshot(temp_path, "attempt1.log")
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=snapshot)])
    for status in (Result.Status.OK, Result.Status.FAIL, Result.Status.ERROR):
        for children in ([Result.Status.OK], [Result.Status.FAIL]):
            result = _result(status=status, children=children)
            labels_before = list(result.get_labels())

            job.report_rabbitmq_recreations(result)
            assert result.status == status
            assert list(result.get_labels()) == labels_before
            assert not result.has_label(Result.Label.INFRA)
            assert result.files == [_attached(temp_path, snapshot)], (status, children)


def test_report_survives_serialization(temp_path):
    """praktika publishes the result through `result_*.json`, so the count and the
    attachment have to reach the persisted form, not just the in-memory object."""
    path = _snapshot(temp_path, "attempt1.log")
    _write_log(temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=path)])
    result = _result(children=[Result.Status.OK])

    job.report_rabbitmq_recreations(result)
    restored = Result.from_dict(json.loads(result.to_json()))
    assert "recreation was attempted 1 time(s)" in restored.info
    assert "Failures: 0/1" in restored.info
    assert _attached(temp_path, path) in [str(f) for f in restored.files]
    assert restored.status == Result.Status.OK


# --- the scan inputs belong to one job -------------------------------------------------
#
# The reporter reads whole files and the per-worker handlers open them in append mode, so
# the count is only "this job's recreations" while nothing older is present. CI gets that
# for free (the workflow removes the temp dir before every job), a local run does not:
# `ci/praktika/runner.py` only creates the directory. Hence the explicit clear.


def test_second_job_over_one_temp_path_reports_nothing(temp_path):
    """The token of a finished job must not be replayed by the next one over the same
    directory: without the clear, a local re-run with no recreation reports one."""
    _write_log(
        temp_path,
        "pytest_parallel-gw0.log",
        [_token_line(snapshot=_snapshot(temp_path, "attempt1.log"))],
    )
    assert job.report_rabbitmq_recreations(_result(children=[Result.Status.OK])) == 1

    job.clear_rabbitmq_recreation_scan_inputs()

    second = _result(children=[Result.Status.OK])
    assert job.report_rabbitmq_recreations(second) == 0
    assert second.info == ""
    assert second.files == []


def test_clear_removes_the_preserved_snapshots_too(temp_path):
    """A stale snapshot is an S3 upload of another job's broker log, and it is what makes
    a replayed token look attachable rather than merely counted."""
    snapshot = temp_path / "rabbit-proj-pid1-call1-attempt1.log"
    snapshot.write_text("stale broker log\n")
    _write_log(
        temp_path, "pytest_parallel-gw0.log", [_token_line(snapshot=snapshot.name)]
    )

    job.clear_rabbitmq_recreation_scan_inputs()

    assert not snapshot.exists()
    assert list(temp_path.glob("pytest_*.log")) == []


def test_clear_keeps_files_it_does_not_own(temp_path):
    """It runs before the batches, so it must not delete artifacts other collectors
    already placed in the same directory."""
    (temp_path / "dmesg.log").write_text("x\n")
    (temp_path / "pytest_parallel.jsonl").write_text("x\n")
    (temp_path / "logs.tar.gz").write_text("x\n")

    job.clear_rabbitmq_recreation_scan_inputs()

    assert (temp_path / "dmesg.log").exists()
    assert (temp_path / "pytest_parallel.jsonl").exists()
    assert (temp_path / "logs.tar.gz").exists()


def test_clear_is_best_effort(temp_path):
    """An entry that cannot be removed must not raise out of `main`, where the call is a
    bare statement with no handler of its own."""
    # a directory raises IsADirectoryError, an OSError, out of os.remove
    (temp_path / "pytest_parallel-gw0.log").mkdir()
    (temp_path / "pytest_parallel-gw1.log").write_text("removable\n")

    job.clear_rabbitmq_recreation_scan_inputs()

    assert (temp_path / "pytest_parallel-gw0.log").exists()
    assert not (temp_path / "pytest_parallel-gw1.log").exists()


def test_clear_on_a_missing_temp_path_does_not_raise(monkeypatch, tmp_path):
    """The very first job on a fresh checkout has no `ci/tmp` yet."""
    monkeypatch.setattr(job, "temp_path", str(tmp_path / "never-created"))
    job.clear_rabbitmq_recreation_scan_inputs()


# --- the reporter is wired into the job ------------------------------------------------
#
# The arms above all call the reporter directly, so they hold whether or not the job
# ever calls it. `main` needs Docker, a built binary and the praktika environment, so
# it cannot be executed here; the wiring is read out of the shipped source instead.


def _call_name(node):
    """The called name of a Call node, for both `f()` and `obj.f()`."""
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name):
            return node.func.id
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
    return None


def _job_module():
    with open(job.__file__, encoding="utf-8") as f:
        return ast.parse(f.read())


def _definition(module, name):
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} is not defined at module level in {job.__file__}")


def _call_sites(module, name, outside=None):
    skip = {id(n) for n in ast.walk(outside)} if outside is not None else set()
    return [
        node
        for node in ast.walk(module)
        if _call_name(node) == name and id(node) not in skip
    ]


def _unconditional_index(func, name):
    """Index of the statement in `func.body` that is a bare call to `name`.

    A call nested in an `if` / `for` / `try` is not one of these, so this also pins
    that the call is unconditional.
    """
    return [
        i
        for i, stmt in enumerate(func.body)
        if isinstance(stmt, ast.Expr) and _call_name(stmt.value) == name
    ]


def _batch_indices(func):
    """Indices of the statements in `func.body` that run a pytest batch."""
    return [
        i
        for i, stmt in enumerate(func.body)
        if any(_call_name(node) == BATCH for node in ast.walk(stmt))
    ]


def _receiver_name(func, name):
    """The name the call to `name` is made on: `R` in `R.sort().complete_job(...)`."""
    node = func.body[_unconditional_index(func, name)[0]].value
    while True:
        if isinstance(node, ast.Call):
            node = node.func
        elif isinstance(node, ast.Attribute):
            node = node.value
        else:
            break
    assert isinstance(node, ast.Name), ast.dump(node)
    return node.id


def test_reporter_is_called_exactly_once_by_the_job():
    """Every other arm here calls the reporter directly, so all of them stay green if
    the production call is deleted. Counting the call sites is what notices."""
    module = _job_module()
    sites = _call_sites(module, REPORTER, outside=_definition(module, REPORTER))
    assert len(sites) == 1, [node.lineno for node in sites]


def test_reporter_runs_before_complete_job_in_the_same_function():
    """`complete_job` -> `_add_job_summary_to_info` writes `Failures: N/M` only while
    `info` is empty, so a reporter that ran after it would append to a non-empty `info`
    and the failure count would never be written."""
    module = _job_module()
    caller = None
    for func in ast.walk(module):
        if isinstance(func, ast.FunctionDef) and _unconditional_index(func, REPORTER):
            caller = func
    assert caller is not None, f"{REPORTER} is not called as a plain statement"

    reporter_at = _unconditional_index(caller, REPORTER)
    complete_at = _unconditional_index(caller, "complete_job")
    assert len(reporter_at) == 1, reporter_at
    assert (
        len(complete_at) == 1
    ), f"complete_job is not called once in {caller.name}: {complete_at}"
    assert reporter_at[0] < complete_at[0], (
        f"{REPORTER} must run before complete_job in {caller.name}: "
        f"complete_job fills in the failure summary only while info is empty"
    )


def test_reporter_runs_after_every_pytest_batch():
    """The per-worker log handlers append and a sequential batch reuses one log across
    repeats, so a reporter placed between batches would count earlier tokens again and
    miss later ones. Being last in the function is what keeps one scan complete."""
    module = _job_module()
    caller = _definition(module, "main")
    reporter_at = _unconditional_index(caller, REPORTER)[0]
    assert _batch_indices(caller), "no pytest batch found in main"
    assert reporter_at > max(_batch_indices(caller)), (
        reporter_at,
        _batch_indices(caller),
    )


def test_reporter_publishes_the_job_result_object():
    """The name is not enough: a call passing a throwaway `Result` publishes nothing,
    because the object `complete_job` later serializes is never touched. The expected
    name is read off that `complete_job` call rather than hard-coded, so renaming the
    variable stays green while passing a different object does not."""
    caller = _definition(_job_module(), "main")
    published = _receiver_name(caller, "complete_job")
    call = caller.body[_unconditional_index(caller, REPORTER)[0]].value

    assert len(call.args) == 1 and not call.keywords, ast.dump(call)
    assert isinstance(call.args[0], ast.Name), ast.dump(call.args[0])
    assert call.args[0].id == published, (
        f"{REPORTER} must be passed {published}, the object complete_job publishes, "
        f"not {ast.dump(call.args[0])}"
    )


def test_reporter_is_the_statement_immediately_before_complete_job():
    """Between the pytest batches and `complete_job`, `main` rewrites child statuses for
    LLVM coverage, sets its own `info` on those paths, and runs the bugfix-validation
    inversion. A reporter placed before any of that would publish a `Failures: N/M` that
    those writers then contradict, and would consume the empty `info` they rely on."""
    caller = _definition(_job_module(), "main")
    reporter_at = _unconditional_index(caller, REPORTER)[0]
    complete_at = _unconditional_index(caller, "complete_job")[0]

    assert reporter_at == complete_at - 1, (
        f"{REPORTER} must be the statement immediately before complete_job "
        f"(reporter at {reporter_at}, complete_job at {complete_at})"
    )


def test_scan_inputs_are_cleared_once_before_the_first_batch():
    """The reporter reads whole files that the handlers append to, so a stale log from an
    earlier job over the same directory counts as an event of this one. Clearing after a
    batch would instead delete this job's own evidence."""
    module = _job_module()
    caller = _definition(module, "main")
    sites = _call_sites(module, CLEANER, outside=_definition(module, CLEANER))
    assert len(sites) == 1, [node.lineno for node in sites]

    clear_at = _unconditional_index(caller, CLEANER)
    assert len(clear_at) == 1, clear_at
    batches = _batch_indices(caller)
    assert batches, "no pytest batch found in main"
    assert clear_at[0] < min(batches), (clear_at, batches)
