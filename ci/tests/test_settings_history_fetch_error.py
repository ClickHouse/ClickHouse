"""Tests for the settings-history diff fetch in
`ci.jobs.scripts.workflow_hooks.store_data`.

The merge-blocking `Style check / settings_changes_history` validator cannot run
without the diff of `src/Core/SettingsChangesHistory.cpp`, so when the fetch fails
the reported reason has to name the actual cause. Before this was fixed a command
failure returned an empty string, which the next check relabelled as GitHub's
large-diff case, and nothing wrote the `settings_history_fetch_error` key the
style check reads, so the report showed a generic fallback for an auth failure or
a 502 alike. These tests pin one message per cause, pin that a transient failure
is retried rather than fatal, and pin the `strict=False` default that every other
`GH.get_output_with_retries` caller relies on.

The real helper runs against a fake `gh` on `PATH` so the actual subprocess and
retry code is exercised; only `time.sleep` is neutralized.
"""

import ast
import os
import stat
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `store_data` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.praktika.gh as gh_mod
from ci.jobs.scripts.workflow_hooks.store_data import (
    SETTINGS_HISTORY_FILE,
    fetch_settings_history_patch,
    settings_history_fetch_error_message,
    store_settings_history_changes,
)
from ci.praktika.gh import GH
from ci.praktika.settings import Settings

_REPO = "ClickHouse/ClickHouse"
_PR = 12345
_PATCH = '@@ -1,2 +1,3 @@\n+    {"some_setting", false, true, "reason"},\n'


@pytest.fixture
def fake_gh(tmp_path, monkeypatch):
    """Install a fake `gh` on PATH; returns an installer, an invocation counter and the argv.

    The counter is what distinguishes "retried" from "gave up": asserting only the
    final message cannot tell those apart. The recorded argv is what pins the command
    actually issued: a fake that ignores its arguments constrains nothing, so pointing
    the fetch at the wrong PR or the wrong file would otherwise stay green.
    """
    counter = tmp_path / "invocations"
    argv_log = tmp_path / "argv"
    patch_file = tmp_path / "patch.diff"
    patch_file.write_text(_PATCH)

    def install(body, exit_code):
        script = tmp_path / "gh"
        script.write_text(
            "#!/bin/bash\n"
            f'echo x >> "{counter}"\n'
            f'printf "%s\\n" "$*" >> "{argv_log}"\n'
            f"{body}\n"
            f"exit {exit_code}\n"
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # Otherwise every failing case pays the real 4+8+16s backoff ladder.
    monkeypatch.setattr(gh_mod.time, "sleep", lambda _delay: None)

    def invocations():
        return len(counter.read_text().splitlines()) if counter.exists() else 0

    def argv():
        """The argument line of the last `gh` invocation."""
        return argv_log.read_text().splitlines()[-1] if argv_log.exists() else ""

    install.invocations = invocations
    install.argv = argv
    install.emit_patch = f'cat "{patch_file}"'
    return install


def _fetch():
    return fetch_settings_history_patch(_REPO, _PR)


class _FakeInfo:
    """Stand-in for `praktika.info.Info`, exposing only what the hook reads and writes."""

    def __init__(self, pr_number=_PR, is_merge_queue_event=False, linked_pr_number=0):
        self.pr_number = pr_number
        self.is_merge_queue_event = is_merge_queue_event
        self.linked_pr_number = linked_pr_number
        self.repo_name = _REPO
        self.kv = {}

    def store_kv_data(self, key, value):
        self.kv[key] = value


def _style_check_report(kv):
    """check_style.py:609-616 verbatim, so the assertions pin what a reviewer sees."""
    fetch_error = kv.get("settings_history_fetch_error")
    changed = kv.get("settings_history_changed_settings")
    if fetch_error or changed is None:
        return (
            f"{SETTINGS_HISTORY_FILE} changed but its diff could not be fetched to validate "
            f"the settings history (the check must not be skipped when the file changed). "
            f"Error: {fetch_error or 'no data recorded by the store_data.py workflow hook'}."
        )
    return ""


# --- a command failure names the command failure, not the large-diff case ----


def test_auth_failure_names_credentials_and_does_not_retry(fake_gh):
    fake_gh('echo "gh: Bad credentials (HTTP 401)" >&2', 1)
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    message = str(excinfo.value)
    assert "Bad credentials" in message
    assert "exit_code:[1]" in message
    assert "very large diffs" not in message
    # Non-retryable class: retrying a bad token cannot help.
    assert fake_gh.invocations() == 1


def test_server_error_names_the_5xx_and_retries(fake_gh):
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 1)
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    message = str(excinfo.value)
    assert "502" in message
    assert "exit_code:[1]" in message
    assert "very large diffs" not in message
    assert fake_gh.invocations() == Settings.MAX_RETRIES_GH


def test_transient_failure_recovers_on_the_next_attempt(fake_gh, tmp_path):
    """The false red this removes: one 5xx used to fail a merge-blocking check."""
    marker = tmp_path / "attempted"
    fake_gh(
        f'if [ ! -f "{marker}" ]; then touch "{marker}"; '
        'echo "gh: Server Error (HTTP 502)" >&2; exit 1; fi\n'
        f"{fake_gh.emit_patch}",
        0,
    )
    assert _fetch().strip() == _PATCH.strip()
    assert fake_gh.invocations() == 2


# --- rc=0 cases stay distinguishable from each other -------------------------


def test_null_patch_keeps_the_large_diff_message(fake_gh):
    """GitHub really does omit the patch for a very large diff; `jq -r` prints "null"."""
    fake_gh("echo null", 0)
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    assert "very large diffs" in str(excinfo.value)


def test_empty_output_names_the_absent_file(fake_gh):
    """A jq `select` that matches nothing prints nothing at rc=0, unlike a null patch."""
    fake_gh("true", 0)
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    message = str(excinfo.value)
    assert "absent from the GitHub API file list" in message
    assert "very large diffs" not in message


def test_real_patch_succeeds_unretried(fake_gh):
    fake_gh(fake_gh.emit_patch, 0)
    assert _fetch().strip() == _PATCH.strip()
    assert fake_gh.invocations() == 1


def test_the_issued_command_targets_the_right_pr_and_file(fake_gh):
    """Assert the recorded argv, not the message: the fetch could name the wrong PR or the
    wrong file and every message-only assertion would still pass."""
    fake_gh(fake_gh.emit_patch, 0)
    _fetch()
    argv = fake_gh.argv()
    assert f"repos/{_REPO}/pulls/{_PR}/files" in argv
    assert "--paginate" in argv
    # Without the `select` the parser would be handed every changed file's patch.
    assert f'select(.filename == "{SETTINGS_HISTORY_FILE}")' in argv
    assert ".patch" in argv


def test_unresolvable_pr_number_is_reported_without_calling_gh(fake_gh):
    fake_gh(fake_gh.emit_patch, 0)
    with pytest.raises(RuntimeError) as excinfo:
        fetch_settings_history_patch(_REPO, 0)
    assert "could not resolve the PR number" in str(excinfo.value)
    assert fake_gh.invocations() == 0


# --- the message reaches the style check's report ----------------------------


def test_hook_stores_the_cause_so_the_style_check_reports_it(fake_gh):
    """The gap this closes: the key check_style.py reads had no writer, so a real cause
    was replaced by the generic 'no data recorded' fallback."""
    fake_gh('echo "gh: Bad credentials (HTTP 401)" >&2', 1)
    info = _FakeInfo()
    store_settings_history_changes(info)
    assert "settings_history_fetch_error" in info.kv
    report = _style_check_report(info.kv)
    assert "Bad credentials" in report
    assert "no data recorded" not in report


def test_hook_does_not_raise_so_the_remaining_kv_data_is_still_stored(fake_gh):
    """The `changed_files` storage below this block, and the jobs that read it, must
    survive a fetch failure."""
    fake_gh('echo "gh: Bad credentials (HTTP 401)" >&2', 1)
    store_settings_history_changes(_FakeInfo())  # must not raise


def test_hook_stores_the_changed_settings_on_success(fake_gh, tmp_path, monkeypatch):
    """The success path still reaches the parser, and then the check has nothing to report."""
    history = tmp_path / "SettingsChangesHistory.cpp"
    history.write_text(
        'addSettingsChanges(settings_changes_history, "26.7",\n'
        '    {\n'
        '    });\n'
    )
    fake_gh(
        'printf \'%s\\n\' "@@ -2,0 +2,1 @@" '
        '\'+    {"some_setting", false, true, "reason"},\'',
        0,
    )
    info = _FakeInfo()
    store_settings_history_changes(info, path=str(history))
    assert info.kv["settings_history_changed_settings"] == [
        {"namespace": "Session", "name": "some_setting"}
    ]
    assert "settings_history_fetch_error" not in info.kv
    assert _style_check_report(info.kv) == ""


def test_hook_uses_the_linked_pr_number_in_the_merge_queue(fake_gh):
    """A merge-queue run has PR_NUMBER 0; without the linked number every such run would
    report the unresolvable-PR message instead of validating the file."""
    fake_gh(fake_gh.emit_patch, 0)
    info = _FakeInfo(pr_number=0, is_merge_queue_event=True, linked_pr_number=_PR)
    store_settings_history_changes(info)
    assert "settings_history_fetch_error" not in info.kv
    # The linked number has to reach the command, not merely pass the guard.
    assert f"/pulls/{_PR}/files" in fake_gh.argv()


def test_hook_reports_an_unresolvable_pr_number(fake_gh):
    fake_gh(fake_gh.emit_patch, 0)
    info = _FakeInfo(pr_number=0, is_merge_queue_event=True, linked_pr_number=0)
    store_settings_history_changes(info)
    assert "could not resolve the PR number" in info.kv["settings_history_fetch_error"]


def test_stored_message_is_a_bounded_single_line():
    """It lands on a public report page, so cap the length and flatten the newlines
    the helper's multi-field diagnostic contains."""
    message = settings_history_fetch_error_message(
        RuntimeError("first line\nsecond line " + "x" * 900)
    )
    assert "\n" not in message
    assert message.startswith("first line second line")
    assert len(message) == 500


def test_bounding_keeps_a_cause_that_sits_at_the_end():
    """Pin the property, not the length: asserting only `len == 500` is satisfied by
    head-, tail- and middle-truncation alike, so it cannot tell a bound that keeps the
    cause from one that discards it."""
    message = settings_history_fetch_error_message(
        RuntimeError("out:[" + "x" * 900 + "] err:[gh: Server Error (HTTP 502)]")
    )
    assert len(message) <= 500
    assert "502" in message
    # The elision is visible, so a reader is never shown a silently shortened message.
    assert "elided" in message


def test_large_stdout_with_a_failing_gh_still_names_the_cause(fake_gh):
    """The reachable carrier item 1 was about: `gh api --paginate` streams each page, so a
    5xx on a later page exits non-zero with an earlier page's patch already on stdout. The
    bounded message must still name the 5xx, the exit code and the attempt count."""
    fake_gh(
        f"{fake_gh.emit_patch}\n"
        + "\n".join(f'echo \'+    {{"setting_{i:03d}", false, true, "reason"}},\'' for i in range(20))
        + '\necho "gh: Server Error (HTTP 502)" >&2',
        1,
    )
    info = _FakeInfo()
    store_settings_history_changes(info)
    stored = info.kv["settings_history_fetch_error"]
    assert len(stored) <= 500
    assert "502" in stored
    assert "exit_code:[1]" in stored
    assert f"after [{Settings.MAX_RETRIES_GH}] attempts" in stored
    report = _style_check_report(info.kv)
    assert "502" in report
    assert "no data recorded" not in report


# --- the production entrypoint is wired to the helper ------------------------


_HOOK_PATH = os.path.join(
    os.path.dirname(__file__),
    "../jobs/scripts/workflow_hooks/store_data.py",
)


def _guarded_call_conditions(function_name):
    """The `if` tests guarding every call to `function_name` under the `__main__` block.

    The module is read, not imported: importing it would run the whole hook. Parsed rather
    than grepped so a mention in a comment or a docstring cannot satisfy the assertion.
    """
    with open(_HOOK_PATH, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    main_blocks = [
        node
        for node in tree.body
        if isinstance(node, ast.If) and "__main__" in ast.unparse(node.test)
    ]
    assert len(main_blocks) == 1, "expected exactly one `if __name__ == '__main__':` block"
    conditions = []
    for node in ast.walk(main_blocks[0]):
        if not isinstance(node, ast.If):
            continue
        for inner in ast.walk(node):
            if (
                isinstance(inner, ast.Call)
                and isinstance(inner.func, ast.Name)
                and inner.func.id == function_name
            ):
                conditions.append(ast.unparse(node.test))
    return conditions


def test_main_calls_the_hook_when_the_settings_history_file_changed():
    """Nothing else pins the production wiring: every other cell calls the helpers directly,
    so deleting the call from `__main__` would leave the whole file green."""
    conditions = _guarded_call_conditions("store_settings_history_changes")
    assert conditions, "__main__ does not call store_settings_history_changes"
    assert any(
        "SETTINGS_HISTORY_FILE in changed_files" in condition for condition in conditions
    ), f"the call is not guarded by the changed-file condition: {conditions}"
    assert any(
        "is_merge_queue_event" in condition for condition in conditions
    ), f"the call does not run in the merge queue: {conditions}"


# --- the shared helper's default must not change for its 14 other callers ----


def test_get_output_with_retries_default_returns_empty_string(fake_gh):
    """Carrier for every existing caller: without `strict` a failure still yields ""."""
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 1)
    assert GH.get_output_with_retries("gh api fake") == ""


def test_get_output_with_retries_strict_reports_exit_code_and_stderr(fake_gh):
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 7)
    with pytest.raises(RuntimeError) as excinfo:
        GH.get_output_with_retries("gh api fake", strict=True)
    message = str(excinfo.value)
    assert "exit_code:[7]" in message
    assert "502" in message


def test_the_diagnostic_front_loads_the_cause_and_bounds_each_field(fake_gh):
    """The helper's own guarantee, independent of any caller's bounding: `gh api --paginate`
    streams each page, so a 5xx on a later page exits non-zero with an earlier page already
    on stdout. All 15 call sites print this message and only one of them bounds it, so the
    cause has to come before the API-controlled output and each field has to be capped."""
    fake_gh(
        'head -c 5000 /dev/zero | tr "\\0" "x"\necho "gh: Server Error (HTTP 502)" >&2', 1
    )
    with pytest.raises(RuntimeError) as excinfo:
        GH.get_output_with_retries("gh api fake", strict=True)
    message = str(excinfo.value)
    assert "502" in message[:300], "the cause is not front-loaded"
    assert message.index("err:[") < message.index("out:[")
    # One huge response must not bury the rest of the diagnostic in a log.
    assert len(message) < 1500, f"diagnostic is unbounded ({len(message)} chars)"
    assert "elided" in message
