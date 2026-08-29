"""
Tests for the interim result checkpoint in `ci/jobs/integration_test_job.py`.

`main` writes the results collected so far into its existing result file before any
post-processing, so a job script that dies in that window leaves them on disk instead of
the empty `RUNNING` stub. Three properties are load-bearing and none is obvious from the
call:

* it must UPDATE the existing result rather than build a new one. `Result.create_from`
  takes no `ext`, so a fresh result would drop `ext["on_error_hook"]` (set by this same
  job script) and `ext["run_url"]`. `Runner._get_result_object` gates the hook on
  `result.get_on_error_hook()`, so a change meant to preserve information would silently
  disable the log collection that runs on the hard-timeout path.
* it must assign NO status. Every status decision in `main` (flaky/targeted downgrade,
  the dmesg OOM row, infrastructure-error clearing, the bugfix-validation inversion) runs
  after this point, so a status assigned here would be published as final on a killed job
  -- a bugfix-validation job would report its non-inverted verdict. Left `RUNNING`, the
  runner's own `KILLED` patch decides the status, and because `add_error` and `set_status`
  touch only `ext["errors"]` and `status`, the children survive that patch.
* it must actually RUN. The checkpoint is a guarded call, so the structural arms here can
  only show that the call exists; the runtime arms drive the real helper, which is why it
  is a module-level function rather than inline in `main` (the same reason
  `is_empty_best_effort_skip` was extracted).

This is defence in depth, not the primary fix: it recovers a job script that dies while
the runner survives, not the whole runner process being cancelled - nothing local is left
to upload the checkpoint then. The primary fix is bounding the archiving that starves the
upload (`test_shell_timeout_watchdog.py`).
"""

import ast
import os
import subprocess
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.integration_test_job import checkpoint_collected_results
from ci.praktika.result import Result, ResultInfo
from ci.praktika.settings import Settings
from ci.praktika.utils import Utils

_JOB_SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../jobs/integration_test_job.py")
)


# --- the checkpoint runs BEFORE the archiving it protects against ---------------------


_CHECKPOINT_HELPER = "checkpoint_collected_results"


def _find_function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name}() not found in integration_test_job.py")


def _find_main(tree):
    return _find_function(tree, "main")


def _find_checkpoint(tree):
    """The helper that performs the checkpoint.

    Resolved by name so a checkpoint moved back inline reddens here rather than making
    the arms below silently match nothing.
    """
    return _find_function(tree, _CHECKPOINT_HELPER)


def _call_lines(scope, predicate):
    return sorted(
        node.lineno
        for node in ast.walk(scope)
        if isinstance(node, ast.Call) and predicate(node)
    )


def _is_set_results_call(node):
    return isinstance(node.func, ast.Attribute) and node.func.attr == "set_results"


def _is_checkpoint_helper_call(node):
    return isinstance(node.func, ast.Name) and node.func.id == _CHECKPOINT_HELPER


def _is_compress_call(node):
    return (
        isinstance(node.func, ast.Attribute) and node.func.attr == "compress_files_gz"
    )


def _module_int_constants(tree):
    """Module-level `NAME = <int literal>` assignments, by name."""
    values = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not (
            isinstance(node.value, ast.Constant) and isinstance(node.value.value, int)
        ):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                values[target.id] = node.value.value
    return values


def _positive_timeout(call, constants):
    """The `timeout` a call passes, resolved to a positive int, else None.

    Asserting only that the keyword is present is not enough: `timeout=None` keeps the
    keyword and disables the bound, which is the regression this is meant to catch.
    """
    keyword = next((kw for kw in call.keywords if kw.arg == "timeout"), None)
    if keyword is None:
        return None
    value = keyword.value
    if isinstance(value, ast.Constant):
        resolved = value.value
    elif isinstance(value, ast.Name):
        resolved = constants.get(value.id)
    else:
        resolved = None
    if isinstance(resolved, int) and not isinstance(resolved, bool) and resolved > 0:
        return resolved
    return None


# The bound both production call sites promise. Kept here rather than imported, so
# renaming or re-deriving either constant still has to face this number.
_CONTRACTED_TIMEOUT_SEC = 1800


_STATUS_ASSIGNING_METHODS = ("set_status", "set_error", "set_failed", "set_success")


def _status_assignments_in(node):
    return sorted(
        sub.func.attr
        for sub in ast.walk(node)
        if isinstance(sub, ast.Call)
        and isinstance(sub.func, ast.Attribute)
        and sub.func.attr in _STATUS_ASSIGNING_METHODS
    )


def _smallest_enclosing_statement(scope, node):
    """The innermost statement of `scope` containing `node`.

    Innermost rather than any enclosing one: a status call anywhere in main() would
    match an outer statement, so only the statement that actually performs the
    checkpoint can be inspected for one.
    """
    best = None
    for statement in ast.walk(scope):
        if not isinstance(statement, ast.stmt):
            continue
        if not any(sub is node for sub in ast.walk(statement)):
            continue
        span = statement.end_lineno - statement.lineno
        if best is None or span < best[0]:
            best = (span, statement)
    return best[1] if best else None


def test_checkpoint_precedes_the_archiving_in_main():
    """The checkpoint must be dumped before the archiving that can outlive the job."""
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)

    checkpoints = _call_lines(main, _is_checkpoint_helper_call)
    archives = _call_lines(main, _is_compress_call)

    assert checkpoints, (
        f"main() never calls {_CHECKPOINT_HELPER}: the collected results are not "
        "checkpointed before post-processing"
    )
    assert archives, (
        "main() contains no compress_files_gz call: this test no longer measures the "
        "ordering it claims to"
    )
    assert checkpoints[0] < archives[0], (
        f"the first checkpoint is at line {checkpoints[0]} but archiving starts at line "
        f"{archives[0]}: an archiving overrun would again discard the collected results"
    )


def test_main_hands_the_checkpoint_its_collected_results():
    """`main` must pass the results it collected, not some other expression.

    The helper is tested directly below; this is the one property only the call site can
    show. `checkpoint_collected_results(job_name, [], is_local_run)` would satisfy every
    other assertion here while writing the empty report the change exists to prevent.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)

    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and _is_checkpoint_helper_call(node)
    ]
    assert calls, f"main() never calls {_CHECKPOINT_HELPER}"

    # Bound to the helper's own parameter order, so each argument is checked where the
    # helper reads it. Membership alone is satisfied by a call that swaps two arguments,
    # which silently hands the results list to the truthy local-run guard.
    params = [arg.arg for arg in _find_checkpoint(tree).args.args]
    for node in calls:
        bound = dict(zip(params, node.args))
        bound.update({kw.arg: kw.value for kw in node.keywords if kw.arg})

        # The whole expression, not its trailing attribute: `info.job_name` and
        # `undefined.job_name` share the attribute, and the second raises before
        # anything is checkpointed.
        got = {param: ast.unparse(value) for param, value in bound.items()}
        for param, expected in (
            ("test_results", "test_results"),
            ("is_local_run", "info.is_local_run"),
            ("job_name", "info.job_name"),
        ):
            assert got.get(param) == expected, (
                f"the checkpoint call at line {node.lineno} passes {got.get(param)!r} as "
                f"`{param}` rather than `{expected}` (binds {got}); the results would be "
                f"read from somewhere other than the run that collected them"
            )


def test_the_checkpoint_call_is_unconditional():
    """`main` must call the checkpoint on every path, not under a condition of its own.

    The helper owns the only guard, and every other arm drives the helper, so a second
    guard around the CALL SITE would be invisible to all of them while stopping the
    checkpoint from ever running. Asserted as a bare call statement in main's own body.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)

    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and _is_checkpoint_helper_call(node)
    ]
    assert calls, f"main() never calls {_CHECKPOINT_HELPER}"

    top_level = [
        statement
        for statement in main.body
        if isinstance(statement, ast.Expr)
        and isinstance(statement.value, ast.Call)
        and _is_checkpoint_helper_call(statement.value)
    ]
    assert len(top_level) == len(calls), (
        f"only {len(top_level)} of {len(calls)} checkpoint calls are bare statements of "
        "main()'s body; the rest are nested inside a conditional, loop or try, so the "
        "checkpoint would not run on every path -- and the helper already owns the only "
        "guard there should be"
    )


def test_every_archiving_call_site_is_bounded():
    """Every `compress_files_gz` call in main() must pass a positive timeout.

    The bound is opt-in per call site, and the behavioural tests drive the helper
    directly, so they cannot see a call site that forgot it. The VALUE is checked, not
    just the keyword: `timeout=None` keeps the keyword and disables the bound.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)
    constants = _module_int_constants(tree)

    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and _is_compress_call(node)
    ]
    assert calls, "main() contains no compress_files_gz call to check"

    unbounded = [
        node.lineno for node in calls if _positive_timeout(node, constants) is None
    ]
    assert unbounded == [], (
        f"compress_files_gz call sites without a positive timeout at lines {unbounded}: "
        "an overrun there would again discard the collected results"
    )

    # The behavioural arms pass their own short timeouts, so only this can hold the
    # production value. Any positive number keeps them green while a small one discards
    # the healthy archives the bound exists to preserve.
    wrong = {
        node.lineno: _positive_timeout(node, constants)
        for node in calls
        if _positive_timeout(node, constants) != _CONTRACTED_TIMEOUT_SEC
    }
    assert wrong == {}, (
        f"archiving call sites bounded at something other than "
        f"{_CONTRACTED_TIMEOUT_SEC}s: {wrong}; healthy shards archive for up to 5747s "
        f"against an 18000s job budget, so a shorter bound throws away good archives"
    )


def test_the_on_error_hook_is_bounded():
    """The on_error_hook's Shell.check must pass a timeout.

    The hook is the second unbounded archiving path ahead of the upload, and it runs
    even earlier: inside `Runner._get_result_object`, before `_post_run` uploads
    anything.
    """
    runner_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../praktika/runner.py")
    )
    with open(runner_path, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=runner_path)
    constants = _module_int_constants(tree)

    hook_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "check"
        and any(
            isinstance(sub, ast.Attribute) and sub.attr == "get_on_error_hook"
            for sub in ast.walk(node)
        )
    ]
    assert hook_calls, "no Shell.check invocation of the on_error_hook found"

    unbounded = [
        node.lineno for node in hook_calls if _positive_timeout(node, constants) is None
    ]
    assert unbounded == [], (
        f"the on_error_hook is invoked without a positive timeout at lines {unbounded}: "
        "it runs before the result is uploaded, so an overrun costs the whole report"
    )

    wrong = {
        node.lineno: _positive_timeout(node, constants)
        for node in hook_calls
        if _positive_timeout(node, constants) != _CONTRACTED_TIMEOUT_SEC
    }
    assert wrong == {}, (
        f"the on_error_hook is bounded at something other than "
        f"{_CONTRACTED_TIMEOUT_SEC}s: {wrong}; the hook archives every test instance "
        f"directory, so a shorter bound loses the logs it exists to collect"
    )


def test_the_on_error_hook_publishes_its_archive_by_rename():
    """The hook's own tar must write to a temporary name and rename on success.

    The hook writes the same `logs.tar.gz` the normal path produces, so writing the
    destination directly would publish a truncation on the hook's own timeout and destroy
    a complete archive the normal path had already written.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source, filename=_JOB_SCRIPT)

    hooks = [
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_on_error_hook"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ]
    assert hooks, "no on_error_hook literal found in the job script"

    for hook in hooks:
        assert '-czf "$tmp_archive"' in hook, (
            "the on_error_hook archives straight to its destination; a tar cut short by "
            f"the hook timeout would be published as the logs. Hook:\n{hook}"
        )
        # `mktemp`, not `$$`: the job script archives the same path from inside Docker's
        # own pid namespace, so a pid can name the file both writers stage into.
        assert (
            "tmp_archive=$(mktemp ./ci/tmp/logs.tar.gz.XXXXXXXX.tmp)" in hook
        ), f"the hook's temporary archive name is not unique per writer. Hook:\n{hook}"
        assert (
            'mv "$tmp_archive" ./ci/tmp/logs.tar.gz' in hook
        ), f"the on_error_hook never renames its temporary archive in. Hook:\n{hook}"
        assert (
            'rm -f "$tmp_archive"' in hook
        ), f"the hook leaves its temporary archive behind on failure. Hook:\n{hook}"
        # `tar` exit 1 is "some files differ", which the hook's own inputs (a job log
        # still being appended to, live test instance directories) produce routinely.
        # Rejecting it would discard a complete archive.
        assert '"$tar_rc" -le 1' in hook, (
            "the on_error_hook does not accept tar's benign exit 1, so it would discard "
            f"a complete archive whenever an input was appended to. Hook:\n{hook}"
        )
        # And an rc outside 0/1 is not by itself fatal. The hook's `_instances*` glob
        # matches nothing until a cluster starts, and the unexpanded pattern makes tar
        # exit 2 after archiving every input that did exist, so gating on the rc alone
        # discards a complete archive on the very path this hook exists to serve.
        assert 'tar -tzf "$tmp_archive"' in hook, (
            "the on_error_hook rejects on tar's exit code alone: it never checks whether "
            f"the archive reads back, so a complete one is discarded. Hook:\n{hook}"
        )
        probe = hook.index('tar -tzf "$tmp_archive"')
        publish = hook.index('mv "$tmp_archive" ./ci/tmp/logs.tar.gz')
        assert probe < publish, (
            "the on_error_hook reads the archive back only after publishing it, so the "
            f"check cannot gate the rename. Hook:\n{hook}"
        )


def _run_hook(workdir, hook):
    """Run the hook literal verbatim in `workdir`, stubbing what needs privileges.

    The stubs are prepended on PATH rather than edited into the body, so the shell the
    job actually installs is the shell under test. Everything the assertions read - the
    tar, its rc branch, the rename and the cleanup - runs unstubbed and unprivileged.
    """
    stubs = workdir / "stubs"
    stubs.mkdir(exist_ok=True)
    for name in ("dmesg", "sudo"):
        stub = stubs / name
        stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        stub.chmod(0o755)

    env = dict(os.environ, PATH=f"{stubs}:{os.environ['PATH']}")
    return subprocess.run(
        ["bash", "-c", hook],
        cwd=workdir,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def _hook_literals():
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    hooks = [
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_on_error_hook"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    ]
    assert hooks, "no on_error_hook literal found in the job script"
    return hooks


def test_the_on_error_hook_publishes_a_usable_archive_it_could_not_exit_cleanly_on(
    tmp_path,
):
    """The hook must RUN and publish, on the rc its own globs produce.

    The arm above reads tokens only, so it passes over a body that can never reach the
    rename (an `exit 0` before the rc branch keeps every token in place). Here the shell
    runs: with no `_instances` directory the glob stays unexpanded and tar exits 2 having
    archived every input that did exist, which is the path the hook exists to serve.
    """
    for hook in _hook_literals():
        work = tmp_path / "run"
        (work / "ci/tmp").mkdir(parents=True, exist_ok=True)
        (work / "tests/integration").mkdir(parents=True, exist_ok=True)
        (work / "ci/tmp/job.log").write_text("job\n" * 1000, encoding="utf-8")
        (work / "ci/tmp/host_metrics.jsonl").write_text("{}\n", encoding="utf-8")

        proc = _run_hook(work, hook)

        published = work / "ci/tmp/logs.tar.gz"
        assert published.is_file(), (
            "the on_error_hook published no archive on the unexpanded-glob path, which "
            f"is the one it exists to serve. rc={proc.returncode}\n{proc.stdout}\n"
            f"{proc.stderr}"
        )
        listing = subprocess.run(
            ["tar", "-tzf", str(published)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert (
            listing.returncode == 0
        ), f"the published archive does not read back: {listing.stderr}"
        assert (
            "job.log" in listing.stdout
        ), f"the published archive is missing an input that existed: {listing.stdout}"
        assert not list(
            work.glob("ci/tmp/*.tmp")
        ), "the on_error_hook left its temporary archive behind after publishing"


def test_the_on_error_hook_publishes_on_a_real_tar_exit_of_one(tmp_path):
    """The hook must RUN and publish on `tar` exit 1, not only on the glob's exit 2.

    Exit 1 is "some files differ", which every log the job is still appending to
    produces, so it is the hook's most-travelled non-zero rc. The other runtime arms
    both drive exit 2, leaving the `-le 1` half of the rc branch unexecuted: a discard
    inserted for exit 1 alone would keep them green.
    """
    for hook in _hook_literals():
        work = tmp_path / "rc-one"
        (work / "ci/tmp").mkdir(parents=True, exist_ok=True)
        # The `_instances*` glob must expand here, or its unmatched pattern makes tar
        # exit 2 and rc 1 never surfaces.
        (work / "tests/integration/test_x/_instances").mkdir(
            parents=True, exist_ok=True
        )
        (work / "tests/integration/test_x/_instances/node.log").write_text(
            "node\n", encoding="utf-8"
        )
        (work / "ci/tmp/host_metrics.jsonl").write_text("{}\n", encoding="utf-8")
        # Grown while tar reads it, which is what makes tar report 1. Large and
        # incompressible so the write is still in progress when the appender starts.
        live = work / "ci/tmp/job.log"
        live.write_bytes(os.urandom(48 * 1024 * 1024))

        stop = threading.Event()

        def append():
            with open(live, "ab") as f:
                while not stop.is_set():
                    f.write(os.urandom(1024 * 1024))
                    f.flush()

        appender = threading.Thread(target=append, daemon=True)
        appender.start()
        try:
            proc = _run_hook(work, hook)
        finally:
            stop.set()
            appender.join(timeout=30)

        assert "tar rc [1]" in proc.stdout, (
            "the hook did not report tar rc 1, so this arm did not exercise the rc-1 "
            f"path it exists for:\n{proc.stdout}\n{proc.stderr}"
        )
        published = work / "ci/tmp/logs.tar.gz"
        assert published.is_file(), (
            "the on_error_hook published no archive on a tar exit of 1, which every "
            f"still-growing log produces:\n{proc.stdout}\n{proc.stderr}"
        )
        listing = subprocess.run(
            ["tar", "-tzf", str(published)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert (
            listing.returncode == 0
        ), f"the archive published on rc 1 does not read back: {listing.stderr}"
        assert (
            "job.log" in listing.stdout
        ), f"the appended input is not in the archive: {listing.stdout}"
        assert not list(
            work.glob("ci/tmp/*.tmp")
        ), "the on_error_hook left its temporary archive behind after publishing"


def test_the_on_error_hook_rejects_a_truncated_archive_and_keeps_the_published_one(
    tmp_path,
):
    """A truncated archive must never replace one already on disk.

    The hook's own timeout kills tar mid-write, and the upload only checks that the file
    exists, so publishing that truncation would hand the report an unreadable tarball.
    Driven by making the readback fail rather than by racing a timeout.
    """
    for hook in _hook_literals():
        work = tmp_path / "reject"
        (work / "ci/tmp").mkdir(parents=True, exist_ok=True)
        (work / "tests/integration").mkdir(parents=True, exist_ok=True)
        (work / "ci/tmp/job.log").write_text("job\n" * 1000, encoding="utf-8")

        # A `tar` that reports failure and leaves an unreadable staging file, so both
        # the rc branch and the readback gate see the truncation case.
        stubs = work / "stubs"
        stubs.mkdir(exist_ok=True)
        (stubs / "tar").write_text(
            "#!/bin/sh\n"
            'if [ "$1" = "-tzf" ]; then exit 2; fi\n'
            'for a in "$@"; do case "$prev" in -czf) printf garbage >"$a";; esac; '
            'prev="$a"; done\n'
            "exit 2\n",
            encoding="utf-8",
        )
        (stubs / "tar").chmod(0o755)

        keeper = work / "ci/tmp/logs.tar.gz"
        keeper.write_bytes(b"the archive the normal path already published")
        before = keeper.read_bytes()

        _run_hook(work, hook)

        assert (
            keeper.read_bytes() == before
        ), "the on_error_hook replaced a published archive with a truncated one"
        assert not list(
            work.glob("ci/tmp/*.tmp")
        ), "the on_error_hook left its rejected temporary archive behind"


def test_checkpoint_uses_the_existing_result_not_a_fresh_one():
    """The checkpoint must be built with `from_fs`, never `Result.create_from`.

    `create_from` takes no `ext`, so it would drop the on_error_hook and the run url. The
    `create_from` half is asserted as an absence: a rewrite to `create_from(...).dump()`
    has no `set_results` at all, so a receiver-only check would pass over nothing.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    checkpoint = _find_checkpoint(tree)

    constructors = sorted(
        node.func.attr
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in ("from_fs", "create_from")
    )
    assert constructors == ["from_fs"], (
        f"{_CHECKPOINT_HELPER}() builds its result with {constructors} instead of "
        "exactly ['from_fs']; anything but from_fs discards ext, which holds the "
        "on_error_hook and the run url"
    )

    for node in ast.walk(checkpoint):
        if not (isinstance(node, ast.Call) and _is_set_results_call(node)):
            continue
        receiver = node.func.value
        assert isinstance(receiver, ast.Call) and isinstance(
            receiver.func, ast.Attribute
        ), f"unexpected set_results receiver at line {node.lineno}"
        assert receiver.func.attr == "from_fs", (
            f"the checkpoint at line {node.lineno} calls "
            f"`{receiver.func.attr}` instead of `from_fs`; anything but from_fs "
            "discards ext, which holds the on_error_hook and the run url"
        )


def test_checkpoint_persists_the_collected_results():
    """The helper must persist its own argument, not some other expression.

    `set_results([])` satisfies every structural property of the call while writing an
    empty result list, which is precisely the empty report the change exists to prevent.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    checkpoint = _find_checkpoint(tree)
    parameters = [arg.arg for arg in checkpoint.args.args]

    calls = [
        node
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call) and _is_set_results_call(node)
    ]
    assert calls, f"{_CHECKPOINT_HELPER}() contains no set_results call"

    for node in calls:
        assert len(node.args) == 1, (
            f"the checkpoint at line {node.lineno} passes {len(node.args)} positional "
            "arguments; expected exactly the collected results"
        )
        argument = node.args[0]
        assert isinstance(argument, ast.Name) and argument.id in parameters, (
            f"the checkpoint at line {node.lineno} passes "
            f"{ast.dump(argument)} instead of one of its own parameters {parameters}; "
            "anything else can persist an empty report"
        )


def test_checkpoint_assigns_no_status_at_the_call_site():
    """The checkpoint statement must not assign a status.

    Every status decision in main() runs after this point. Asserted structurally too: the
    runtime arm sees only the status the helper leaves, not a `set_status` a later
    refactor puts back on this statement.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    checkpoint = _find_checkpoint(tree)

    calls = [
        node
        for node in ast.walk(checkpoint)
        if isinstance(node, ast.Call) and _is_set_results_call(node)
    ]
    assert calls, f"{_CHECKPOINT_HELPER}() contains no set_results call"

    for node in calls:
        # Anywhere in the same chain, e.g. `.set_results(x).set_status(OK)`.
        chained = _status_assignments_in(node.func.value) + [
            sub.func.attr
            for sub in ast.walk(node)
            if isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr in _STATUS_ASSIGNING_METHODS
        ]
        statement = _smallest_enclosing_statement(checkpoint, node)
        assert statement is not None, f"no statement encloses line {node.lineno}"
        on_statement = _status_assignments_in(statement)
        assert chained == [] and on_statement == [], (
            f"the checkpoint at line {node.lineno} assigns a status "
            f"({sorted(set(chained + on_statement))}); it would pre-empt the "
            "flaky downgrade, the dmesg OOM row, infrastructure-error clearing and the "
            "bugfix-validation inversion, all of which run after this point"
        )


# --- the checkpoint behaves correctly, driving the real Result ------------------------

_SENTINEL_HOOK = "echo checkpoint-hook-sentinel"


def _checkpointed_result(tmp_path, children, is_local_run=False):
    """A RUNNING result with a hook, then checkpointed with `children`. Re-read from fs.

    The checkpoint is performed by calling the job script's own helper, so a checkpoint
    that never runs (a wrong guard, a disabled body) reddens here. Re-implementing the
    setter sequence by hand would pass in that case.
    """
    original_temp_dir = Settings.TEMP_DIR
    Settings.TEMP_DIR = str(tmp_path)
    try:
        name = "Integration tests (checkpoint probe)"
        # Exactly what _pre_run leaves on disk before the job script runs: a bare
        # Result with a start_time (Runner._pre_run), not create_from -- create_from
        # reads start_time from a file that does not exist yet, so a fixture built with
        # it would leave duration None and make the runner-patch arm assert nothing.
        Result(
            name=name,
            status=Result.Status.RUNNING,
            start_time=Utils.timestamp(),
        ).set_on_error_hook(_SENTINEL_HOOK).dump()

        checkpoint_collected_results(name, children, is_local_run)

        return Result.from_fs(name)
    finally:
        Settings.TEMP_DIR = original_temp_dir


def _children(count):
    return [
        Result.create_from(name=f"test_module.py::test_{i}", status=Result.Status.OK)
        for i in range(count)
    ]


def test_checkpoint_persists_the_children(tmp_path):
    """The collected results must be on disk after the checkpoint, with no extra dump.

    This is the arm that catches a checkpoint which never executes. The structural arms
    above see only that the call exists, so a guard that is never true -- or a body
    disabled outright -- satisfies all of them while restoring the empty report.
    """
    reread = _checkpointed_result(tmp_path, _children(3))

    assert len(reread.results) == 3, (
        f"the checkpoint persisted {len(reread.results)} children instead of 3; "
        "set_results did not reach disk"
    )


def test_checkpoint_is_skipped_on_a_local_run(tmp_path):
    """A local run has no runner to publish anything, so nothing is written.

    Pins the guard's polarity. Inverted, this arm sees the children appear and the arm
    above sees them vanish, so the two together fix the guard's direction.
    """
    reread = _checkpointed_result(tmp_path, _children(3), is_local_run=True)

    assert reread.results == [], (
        f"a local run wrote {len(reread.results)} children to the result file; the "
        "guard's polarity is inverted"
    )


def test_checkpoint_assigns_no_status(tmp_path):
    """The checkpoint must stay RUNNING so the harness decides the final status.

    A completed status here would be assigned before the retry block, the dmesg OOM row,
    infrastructure-error clearing and the bugfix-validation inversion have run, and on a
    killed job that half-decided status would be published as the verdict.
    """
    reread = _checkpointed_result(tmp_path, _children(2))

    assert not reread.is_completed(), (
        f"the checkpoint published a completed status [{reread.status}]; it would "
        "pre-empt every status decision that runs after this point"
    )


def test_checkpoint_preserves_the_on_error_hook(tmp_path):
    """`ext` must survive: the hook is what collects logs on the hard-timeout path."""
    reread = _checkpointed_result(tmp_path, _children(2))

    assert reread.get_on_error_hook() == _SENTINEL_HOOK, (
        f"the checkpoint lost the on_error_hook (got {reread.get_on_error_hook()!r}); "
        "Runner._get_result_object gates the hook on it, so log collection would "
        "silently stop running"
    )


def test_runner_patch_turns_the_checkpoint_into_a_populated_error(tmp_path):
    """The measured statement of the fix, where the runner survives the job script.

    Applies the patch sequence from `Runner._get_result_object` to the checkpointed
    result: an incomplete result gets KILLED + ERROR, then its duration is filled. The
    empty `ERROR` becomes a populated `ERROR` -- the children must survive.
    """
    reread = _checkpointed_result(tmp_path, _children(5))
    assert not reread.is_completed(), "precondition: the checkpoint must be incomplete"

    reread.add_error(ResultInfo.KILLED).set_status(Result.Status.ERROR)
    reread.update_duration()

    assert reread.status == Result.Status.ERROR, f"unexpected status {reread.status}"
    assert len(reread.results) == 5, (
        f"the runner's KILLED patch dropped children ({len(reread.results)} of 5 left); "
        "the report would still be empty"
    )
    assert reread.duration is not None, "the runner did not fill in the duration"


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    test_checkpoint_precedes_the_archiving_in_main()
    test_main_hands_the_checkpoint_its_collected_results()
    test_the_checkpoint_call_is_unconditional()
    test_every_archiving_call_site_is_bounded()
    test_the_on_error_hook_is_bounded()
    test_the_on_error_hook_publishes_its_archive_by_rename()
    test_checkpoint_uses_the_existing_result_not_a_fresh_one()
    test_checkpoint_persists_the_collected_results()
    test_checkpoint_assigns_no_status_at_the_call_site()
    for fn in (
        test_checkpoint_persists_the_children,
        test_checkpoint_is_skipped_on_a_local_run,
        test_checkpoint_assigns_no_status,
        test_checkpoint_preserves_the_on_error_hook,
        test_runner_patch_turns_the_checkpoint_into_a_populated_error,
    ):
        with tempfile.TemporaryDirectory() as d:
            fn(Path(d))
        print(f"ok {fn.__name__}")
    print("ok")
