"""
Tests for the interim result checkpoint in `ci/jobs/integration_test_job.py`.

Background
----------
`integration_test_job.main` collected every test result into `test_results` and dumped it
only at the very end, with unbounded post-processing in between, so a job that died in
that window published the pre-run `RUNNING` stub as an `ERROR` with no children.

`main` now writes the collected results into that existing result file before any
post-processing. Two properties are load-bearing and neither is obvious from the call:

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

This is honest defence in depth, not the primary fix: it recovers a job script that dies
while the runner survives (a crash, a non-zero exit, the hard TeePopen timeout). It cannot
recover the whole runner process being cancelled, which is what happened on the evidenced
job -- nothing local is left to upload the checkpoint then. The primary fix is bounding
the archiving that starves the upload (`test_shell_timeout_watchdog.py`).
"""

import ast
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result, ResultInfo
from ci.praktika.settings import Settings
from ci.praktika.utils import Utils

_JOB_SCRIPT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../jobs/integration_test_job.py")
)


# --- the checkpoint runs BEFORE the archiving it protects against ---------------------


def _find_main(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "main":
            return node
    raise AssertionError("main() not found in integration_test_job.py")


def _call_lines(scope, predicate):
    return sorted(
        node.lineno
        for node in ast.walk(scope)
        if isinstance(node, ast.Call) and predicate(node)
    )


def _is_set_results_call(node):
    return isinstance(node.func, ast.Attribute) and node.func.attr == "set_results"


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

    checkpoints = _call_lines(main, _is_set_results_call)
    archives = _call_lines(main, _is_compress_call)

    assert checkpoints, (
        "main() contains no set_results call: the collected results are never "
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


def test_every_archiving_call_site_is_bounded():
    """Every `compress_files_gz` call in main() must pass a positive timeout.

    The bound is what keeps archiving from starving the result upload, and it is opt-in
    per call site (`timeout` defaults to None so other callers are unaffected), so an
    unbounded call site restores the defect for the archive it writes. The behavioural
    tests drive `compress_files_gz` directly and cannot see a call site that forgot it.
    The VALUE is checked, not just the keyword: `timeout=None` keeps the keyword and
    disables the bound.
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


def test_the_on_error_hook_publishes_its_archive_by_rename():
    """The hook's own tar must write to a temporary name and rename on success.

    The hook writes to the same `logs.tar.gz` the normal path produces, and the upload
    only checks that the file exists. Writing the destination directly would publish a
    truncated archive when the hook timeout fires, and would also destroy a complete
    archive the normal path had already written.
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
        assert (
            "tmp_archive=./ci/tmp/logs.tar.gz.$$.tmp" in hook
        ), f"the hook's temporary archive name is not unique per shell. Hook:\n{hook}"
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


def test_checkpoint_uses_the_existing_result_not_a_fresh_one():
    """The checkpoint must be built with `from_fs`, never `Result.create_from`.

    `create_from` takes no `ext`, so it would drop the on_error_hook and the run url.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)

    for node in ast.walk(main):
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
    """The checkpoint must pass the collected results, not some other expression.

    `set_results([])` satisfies every structural property of the call while writing an
    empty result list, which is precisely the empty report the change exists to prevent.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)

    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and _is_set_results_call(node)
    ]
    assert calls, "main() contains no set_results call"

    for node in calls:
        assert len(node.args) == 1, (
            f"the checkpoint at line {node.lineno} passes {len(node.args)} positional "
            "arguments; expected exactly the collected results"
        )
        argument = node.args[0]
        assert isinstance(argument, ast.Name) and argument.id == "test_results", (
            f"the checkpoint at line {node.lineno} passes "
            f"{ast.dump(argument)} instead of the `test_results` collected so far; "
            "anything else can persist an empty report"
        )


def test_checkpoint_assigns_no_status_at_the_call_site():
    """The checkpoint statement must not assign a status.

    Every status decision in main() runs after this point, so a status assigned here
    would be published as final on a killed job. Asserted structurally because the
    behavioural arm drives `Result` directly and cannot see main()'s own call.
    """
    with open(_JOB_SCRIPT, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=_JOB_SCRIPT)
    main = _find_main(tree)

    calls = [
        node
        for node in ast.walk(main)
        if isinstance(node, ast.Call) and _is_set_results_call(node)
    ]
    assert calls, "main() contains no set_results call"

    for node in calls:
        # Anywhere in the same chain, e.g. `.set_results(x).set_status(OK)`.
        chained = _status_assignments_in(node.func.value) + [
            sub.func.attr
            for sub in ast.walk(node)
            if isinstance(sub, ast.Call)
            and isinstance(sub.func, ast.Attribute)
            and sub.func.attr in _STATUS_ASSIGNING_METHODS
        ]
        statement = _smallest_enclosing_statement(main, node)
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


def _checkpointed_result(tmp_path, children):
    """A RUNNING result with a hook, then checkpointed with `children`. Re-read from fs."""
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

        # The checkpoint, exactly as main() performs it: no trailing .dump(), because
        # set_results dumps through _dump_if_persisted once the file exists.
        Result.from_fs(name).set_results(children)

        return Result.from_fs(name)
    finally:
        Settings.TEMP_DIR = original_temp_dir


def _children(count):
    return [
        Result.create_from(name=f"test_module.py::test_{i}", status=Result.Status.OK)
        for i in range(count)
    ]


def test_checkpoint_persists_the_children(tmp_path):
    """The collected results must be on disk after the checkpoint, with no extra dump."""
    reread = _checkpointed_result(tmp_path, _children(3))

    assert len(reread.results) == 3, (
        f"the checkpoint persisted {len(reread.results)} children instead of 3; "
        "set_results did not reach disk"
    )


def test_checkpoint_assigns_no_status(tmp_path):
    """The checkpoint must stay RUNNING so the harness decides the final status.

    Asserting `is_completed()` is False is deliberate: a completed status here would be
    assigned before the retry block, the dmesg OOM row, infrastructure-error clearing and
    the bugfix-validation inversion have run, and on a killed job that half-decided
    status would be published as the verdict.
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
    test_every_archiving_call_site_is_bounded()
    test_the_on_error_hook_is_bounded()
    test_the_on_error_hook_publishes_its_archive_by_rename()
    test_checkpoint_uses_the_existing_result_not_a_fresh_one()
    test_checkpoint_persists_the_collected_results()
    test_checkpoint_assigns_no_status_at_the_call_site()
    for fn in (
        test_checkpoint_persists_the_children,
        test_checkpoint_assigns_no_status,
        test_checkpoint_preserves_the_on_error_hook,
        test_runner_patch_turns_the_checkpoint_into_a_populated_error,
    ):
        with tempfile.TemporaryDirectory() as d:
            fn(Path(d))
        print(f"ok {fn.__name__}")
    print("ok")
