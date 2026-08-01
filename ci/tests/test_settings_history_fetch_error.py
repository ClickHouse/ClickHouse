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
    """Install a fake `gh` on PATH; returns an installer plus an invocation counter.

    The counter is what distinguishes "retried" from "gave up": asserting only the
    final message cannot tell those apart.
    """
    counter = tmp_path / "invocations"
    patch_file = tmp_path / "patch.diff"
    patch_file.write_text(_PATCH)

    def install(body, exit_code):
        script = tmp_path / "gh"
        script.write_text(
            "#!/bin/bash\n"
            f'echo x >> "{counter}"\n'
            f"{body}\n"
            f"exit {exit_code}\n"
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # Otherwise every failing case pays the real 4+8+16s backoff ladder.
    monkeypatch.setattr(gh_mod.time, "sleep", lambda _delay: None)

    def invocations():
        return len(counter.read_text().splitlines()) if counter.exists() else 0

    install.invocations = invocations
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
