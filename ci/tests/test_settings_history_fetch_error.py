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
import json
import os
import stat
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `store_data` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.praktika.gh as gh_mod
from ci.jobs import check_style
from ci.jobs.scripts.workflow_hooks.store_data import (
    SETTINGS_HISTORY_FILE,
    fetch_settings_history_patch_and_file,
    settings_history_fetch_error_message,
    store_settings_history_changes,
)
from ci.praktika.gh import GH
from ci.praktika.settings import Settings

_REPO = "ClickHouse/ClickHouse"
_PR = 12345
_PATCH = '@@ -1,2 +1,3 @@\n+    {"some_setting", false, true, "reason"},\n'
_CONTENTS_URL = f"https://api.github.com/repos/{_REPO}/contents/{SETTINGS_HISTORY_FILE}?ref=deadbeef"
_HEAD_FILE = (
    'addSettingsChanges(settings_changes_history, "26.7",\n'
    "    {\n"
    "    });\n"
)
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))


@pytest.fixture
def fake_gh(tmp_path, monkeypatch):
    """Install a fake `gh` on PATH; returns an installer, an invocation counter and the argv.

    The fetch issues two API reads - the PR's file entry, then that entry's
    `contents_url` - so the fake dispatches on the raw-content Accept header and each
    read has its own body and exit code. A fake that answered both alike could not tell
    a failure of one from a failure of the other.

    The counter is what distinguishes "retried" from "gave up": asserting only the
    final message cannot tell those apart. The recorded argv is what pins the commands
    actually issued: a fake that ignores its arguments constrains nothing, so pointing
    the fetch at the wrong PR, file or revision would otherwise stay green.
    """
    counter = tmp_path / "invocations"
    argv_log = tmp_path / "argv"

    def entry_body(patch=_PATCH, contents_url=_CONTENTS_URL):
        """A body emitting the `{patch, contents_url}` object the jq filter produces."""
        entry = tmp_path / f"entry-{abs(hash((patch, contents_url)))}.json"
        entry.write_text(json.dumps({"patch": patch, "contents_url": contents_url}) + "\n")
        return f'cat "{entry}"'

    def file_body(text=_HEAD_FILE):
        content = tmp_path / f"head-{abs(hash(text))}.cpp"
        content.write_text(text)
        return f'cat "{content}"'

    def install(body, exit_code, contents_body=None, contents_exit_code=0):
        script = tmp_path / "gh"
        script.write_text(
            "#!/bin/bash\n"
            f'echo x >> "{counter}"\n'
            f'printf "%s\\n" "$*" >> "{argv_log}"\n'
            'if [[ "$*" == *vnd.github.raw* ]]; then\n'
            f"{contents_body if contents_body is not None else file_body()}\n"
            f"exit {contents_exit_code}\n"
            "fi\n"
            f"{body}\n"
            f"exit {exit_code}\n"
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # Otherwise every failing case pays the real 4+8+16s backoff ladder.
    monkeypatch.setattr(gh_mod.time, "sleep", lambda _delay: None)

    def invocations():
        return len(counter.read_text().splitlines()) if counter.exists() else 0

    def argv_all():
        return argv_log.read_text().splitlines() if argv_log.exists() else []

    def argv():
        """The argument line of the last `gh` invocation."""
        return argv_all()[-1] if argv_all() else ""

    def files_argv():
        """The argument line of the PR-file-list read, whatever ran after it."""
        matching = [line for line in argv_all() if "/files" in line]
        return matching[-1] if matching else ""

    install.invocations = invocations
    install.argv = argv
    install.argv_all = argv_all
    install.files_argv = files_argv
    install.entry_body = entry_body
    install.file_body = file_body
    install.emit_patch = entry_body()
    return install


def _fetch():
    """The patch alone; the cells that care about the file lines read the pair directly."""
    return fetch_settings_history_patch_and_file(_REPO, _PR)[0]


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
    """A local copy of the reporting branch, used as a cheap oracle by the cells below.

    It is only a copy: `test_the_real_style_check_reports_the_stored_cause` drives the real
    `check_settings_changes_history` so a change to that branch cannot leave this file green.
    """
    fetch_error = kv.get("settings_history_fetch_error")
    changed = kv.get("settings_history_changed_settings")
    if fetch_error or changed is None:
        return (
            f"{SETTINGS_HISTORY_FILE} changed but its diff could not be fetched to validate "
            f"the settings history (the check must not be skipped when the file changed). "
            f"Error: {fetch_error or 'no data recorded by the store_data.py workflow hook'}."
        )
    return ""


class _FakeStyleInfo:
    """Stand-in for `praktika.info.Info` as `check_style` uses it: called, then read.

    Same shape as the one `test_settings_history_source_gate.py` installs.
    """

    def __init__(self, kv):
        self._kv = kv

    def __call__(self):
        return self

    def get_kv_data(self):
        return self._kv


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
    # The count the message REPORTS has to agree with the count of invocations made: a
    # report claiming zero attempts next to a nonzero exit code contradicts itself and
    # sends a maintainer looking at the wiring instead of the token.
    assert f"after [{fake_gh.invocations()}] attempts" in message


def test_server_error_names_the_5xx_and_retries(fake_gh):
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 1)
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    message = str(excinfo.value)
    assert "502" in message
    assert "exit_code:[1]" in message
    assert "very large diffs" not in message
    assert fake_gh.invocations() == Settings.MAX_RETRIES_GH
    assert f"after [{fake_gh.invocations()}] attempts" in message


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
    # One failed entry read, its retry, then the contents read.
    assert fake_gh.invocations() == 3


# --- rc=0 cases stay distinguishable from each other -------------------------


def test_null_patch_keeps_the_large_diff_message(fake_gh):
    """GitHub really does omit the patch for a very large diff; the field is then null."""
    fake_gh(fake_gh.entry_body(patch=None), 0)
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
    patch, file_lines = fetch_settings_history_patch_and_file(_REPO, _PR)
    assert patch.strip() == _PATCH.strip()
    # The lines must come from the fetched head revision, not from the checked-out file.
    assert file_lines == _HEAD_FILE.splitlines()
    # One read for the file entry, one for its contents; neither retried.
    assert fake_gh.invocations() == 2


def test_the_issued_command_targets_the_right_pr_and_file(fake_gh):
    """Assert the recorded argv, not the message: the fetch could name the wrong PR or the
    wrong file and every message-only assertion would still pass."""
    fake_gh(fake_gh.emit_patch, 0)
    _fetch()
    argv = fake_gh.files_argv()
    assert f"repos/{_REPO}/pulls/{_PR}/files" in argv
    assert "--paginate" in argv
    # Without the `select` the parser would be handed every changed file's patch.
    assert f'select(.filename == "{SETTINGS_HISTORY_FILE}")' in argv
    assert "patch" in argv
    # Both halves must come from the same entry, or the patch's line numbers and the file
    # they are resolved against can be two different revisions.
    assert "contents_url" in argv


def test_the_file_is_read_from_the_revision_the_patch_names(fake_gh):
    """The jq output pins which revision to read; a fetch that ignored `contents_url` and
    re-derived the URL could read a different commit and keep every other cell green."""
    other = _CONTENTS_URL.replace("deadbeef", "cafef00d")
    fake_gh(fake_gh.entry_body(contents_url=other), 0)
    _fetch()
    raw_reads = [line for line in fake_gh.argv_all() if "vnd.github.raw" in line]
    assert len(raw_reads) == 1
    assert other in raw_reads[0]


def test_empty_head_file_is_reported_rather_than_parsed_as_empty(fake_gh):
    """An empty file would make the parser attribute every entry to no block at all, so it
    must fail closed with its own message instead of reaching the parser."""
    fake_gh(fake_gh.emit_patch, 0, contents_body="true")
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    assert "no content returned" in str(excinfo.value)


def test_a_failing_contents_read_names_the_cause_and_retries(fake_gh):
    """The second read is jointly load-bearing: a 5xx there used to be laundered into an
    empty string, which the emptiness check would then mislabel."""
    fake_gh(
        fake_gh.emit_patch,
        0,
        contents_body='echo "gh: Server Error (HTTP 502)" >&2',
        contents_exit_code=1,
    )
    with pytest.raises(RuntimeError) as excinfo:
        _fetch()
    message = str(excinfo.value)
    assert "502" in message
    assert "exit_code:[1]" in message
    # One entry read plus the whole retry ladder for the contents read.
    assert fake_gh.invocations() == 1 + Settings.MAX_RETRIES_GH


def test_unresolvable_pr_number_is_reported_without_calling_gh(fake_gh):
    fake_gh(fake_gh.emit_patch, 0)
    with pytest.raises(RuntimeError) as excinfo:
        fetch_settings_history_patch_and_file(_REPO, 0)
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


def test_the_real_style_check_reports_the_stored_cause(fake_gh, monkeypatch):
    """The end-to-end claim, pinned against the production consumer rather than a copy of it.

    Every other cell here asserts through `_style_check_report`, so dropping the
    `fetch_error` term from `check_style.check_settings_changes_history` would leave this
    file green while the report went back to the generic fallback. The second changed file
    is required: without a default-bearing source the check returns early by design.
    """
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 1)
    info = _FakeInfo()
    store_settings_history_changes(info)

    kv = {
        "changed_files": [SETTINGS_HISTORY_FILE, "src/Core/Defines.h"],
        "settings_history_fetch_error": info.kv["settings_history_fetch_error"],
    }
    monkeypatch.setattr(check_style, "Info", _FakeStyleInfo(kv))
    monkeypatch.chdir(_REPO_ROOT)
    report = check_style.check_settings_changes_history()

    assert "502" in report
    assert "no data recorded" not in report


def test_hook_does_not_raise_so_the_remaining_kv_data_is_still_stored(fake_gh):
    """The `changed_files` storage below this block, and the jobs that read it, must
    survive a fetch failure."""
    fake_gh('echo "gh: Bad credentials (HTTP 401)" >&2', 1)
    store_settings_history_changes(_FakeInfo())  # must not raise


def test_hook_stores_the_changed_settings_on_success(fake_gh):
    """The success path still reaches the parser, and then the check has nothing to report."""
    fake_gh(
        fake_gh.entry_body(
            patch='@@ -2,0 +2,1 @@\n+    {"some_setting", false, true, "reason"},\n'
        ),
        0,
    )
    info = _FakeInfo()
    store_settings_history_changes(info)
    assert info.kv["settings_history_changed_settings"] == [
        {"namespace": "Session", "name": "some_setting"}
    ]
    assert "settings_history_fetch_error" not in info.kv
    assert _style_check_report(info.kv) == ""


def test_the_hook_never_reads_the_checked_out_file(fake_gh, tmp_path, monkeypatch):
    """The bug master fixed: resolving the patch against the CI checkout attributes entries
    to the wrong block. Point `path` at a decoy whose blocks disagree with the fetched head
    file - the reported namespace must come from the fetched one."""
    decoy = tmp_path / "SettingsChangesHistory.cpp"
    decoy.write_text(
        'addSettingsChanges(merge_tree_settings_changes_history, "26.7",\n'
        "    {\n"
        "    });\n"
    )
    fake_gh(
        fake_gh.entry_body(
            patch='@@ -2,0 +2,1 @@\n+    {"some_setting", false, true, "reason"},\n'
        ),
        0,
    )
    info = _FakeInfo()
    store_settings_history_changes(info, path=str(decoy))
    assert info.kv["settings_history_changed_settings"] == [
        {"namespace": "Session", "name": "some_setting"}
    ]


def test_hook_uses_the_linked_pr_number_in_the_merge_queue(fake_gh):
    """A merge-queue run has PR_NUMBER 0; without the linked number every such run would
    report the unresolvable-PR message instead of validating the file."""
    fake_gh(fake_gh.emit_patch, 0)
    info = _FakeInfo(pr_number=0, is_merge_queue_event=True, linked_pr_number=_PR)
    store_settings_history_changes(info)
    assert "settings_history_fetch_error" not in info.kv
    # The linked number has to reach the command, not merely pass the guard.
    assert f"/pulls/{_PR}/files" in fake_gh.files_argv()


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
        'head -c 5000 /dev/zero | tr "\\0" "x"\n'
        'echo "gh: Server Error (HTTP 502)" >&2',
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


def _guarded_call_statements(function_name):
    """The outermost `if` statements under `__main__` that contain a call to `function_name`.

    The module is read, not imported: importing it would run the whole hook, which fetches
    build digests and master commits. Only the guarding statements are extracted, so they
    can be EXECUTED below: an oracle that merely matched the condition text would accept a
    guard that can never be true.
    """
    with open(_HOOK_PATH, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    main_blocks = [
        node
        for node in tree.body
        if isinstance(node, ast.If) and "__main__" in ast.unparse(node.test)
    ]
    assert len(main_blocks) == 1, "expected exactly one `if __name__ == '__main__':` block"

    def calls(node):
        return any(
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Name)
            and inner.func.id == function_name
            for inner in ast.walk(node)
        )

    # Walk the body, not the block itself: the `__main__` test is a guard too, and executing
    # it here would always be false and never reach the guard under scrutiny.
    guards = [
        n
        for statement in main_blocks[0].body
        for n in ast.walk(statement)
        if isinstance(n, ast.If) and calls(n)
    ]
    # Keep only the outermost, so every intervening guard is executed rather than skipped.
    return [g for g in guards if not any(g is not o and g in ast.walk(o) for o in guards)]


def _run_main_guard(pr_number, is_merge_queue_event, changed_files):
    """Execute the production guard verbatim; return whether it reached the hook call."""
    statements = _guarded_call_statements("store_settings_history_changes")
    assert statements, "__main__ does not call store_settings_history_changes"
    reached = []
    namespace = {
        "info": _FakeInfo(
            pr_number=pr_number, is_merge_queue_event=is_merge_queue_event
        ),
        "changed_files": changed_files,
        "SETTINGS_HISTORY_FILE": SETTINGS_HISTORY_FILE,
        "store_settings_history_changes": lambda *a, **k: reached.append(True),
    }
    for statement in statements:
        exec(  # noqa: S102 - the statement comes from this repo's own hook
            compile(ast.Module(body=[statement], type_ignores=[]), _HOOK_PATH, "exec"),
            namespace,
        )
    return bool(reached)


@pytest.mark.parametrize(
    "pr_number,is_merge_queue_event,changed_files,expected",
    [
        (_PR, False, [SETTINGS_HISTORY_FILE], True),
        # A merge-queue run has PR_NUMBER 0 and must still validate the file.
        (0, True, [SETTINGS_HISTORY_FILE], True),
        # The guard stays load-bearing in both directions, so a hook that always ran
        # would fail these two rather than pass for the wrong reason.
        (_PR, False, ["src/Core/Defines.h"], False),
        (0, False, [SETTINGS_HISTORY_FILE], False),
    ],
)
def test_main_calls_the_hook_exactly_when_it_should(
    pr_number, is_merge_queue_event, changed_files, expected
):
    """Nothing else pins the production wiring: every other cell calls the helpers directly,
    so deleting the call from `__main__` would leave the whole file green. The guard is
    executed rather than pattern-matched, because a guard amended with `and False`, or a call
    nested under `if False:`, keeps every expected substring while never running in CI."""
    assert _run_main_guard(pr_number, is_merge_queue_event, changed_files) is expected


# --- the shared helper's default must not change for its 14 other callers ----


def test_get_output_with_retries_default_returns_empty_string(fake_gh):
    """Carrier for every existing caller: without `strict` a failure still yields ""."""
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 1)
    assert GH.get_output_with_retries("gh api fake") == ""
    # The retrying is half of the contract those callers depend on, and the return value
    # cannot see it: a short circuit such as `if not strict: return ""` also yields "",
    # having run nothing.
    assert fake_gh.invocations() == Settings.MAX_RETRIES_GH


def test_get_output_with_retries_strict_reports_exit_code_and_stderr(fake_gh):
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 7)
    with pytest.raises(RuntimeError) as excinfo:
        GH.get_output_with_retries("gh api fake", strict=True)
    message = str(excinfo.value)
    assert "exit_code:[7]" in message
    assert "502" in message


@pytest.mark.parametrize(
    "stderr_line",
    [
        "gh: Bad credentials (HTTP 401)",
        "gh: Validation Failed (HTTP 422)",
        "gh: Resource not accessible by integration",
    ],
)
def test_the_diagnostic_counts_the_attempt_a_non_retryable_class_made(
    fake_gh, stderr_line
):
    """Every non-retryable class leaves the loop before the retry counter moves, so the
    reported count must come from where the subprocess is invoked, not from that counter."""
    fake_gh(f'echo "{stderr_line}" >&2', 1)
    with pytest.raises(RuntimeError) as excinfo:
        GH.get_output_with_retries("gh api fake", strict=True)
    assert fake_gh.invocations() == 1
    assert "after [1] attempts" in str(excinfo.value)


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
