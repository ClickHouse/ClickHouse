"""
Tests for the source-file gate of the `settings_changes_history` style check in
`ci/jobs/check_style.py`.

A change that touches no C++ source besides `src/Core/SettingsChangesHistory.cpp` cannot have
changed any setting's compiled default, so it is a historical correction and must not be forced
into the current version block. As soon as any other source file changed, the rule is enforced -
including for files that are not the setting declarations themselves, because a default can come
from a constant defined elsewhere (`src/Core/Defines.h` and friends).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `praktika` is imported as a top-level module by the style-check job.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs import check_style
from ci.jobs.scripts.workflow_hooks.store_data import parse_settings_history_changes


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
HISTORY = "src/Core/SettingsChangesHistory.cpp"


class _FakeInfo:
    def __init__(self, kv):
        self._kv = kv

    def __call__(self):
        return self

    def get_kv_data(self):
        return self._kv


def _run(monkeypatch, kv):
    monkeypatch.setattr(check_style, "Info", _FakeInfo(kv))
    monkeypatch.chdir(REPO_ROOT)
    return check_style.check_settings_changes_history()


def _kv(changed_files, changed_settings):
    return {
        "changed_files": changed_files,
        "settings_history_changed_settings": changed_settings,
    }


def test_history_only_change_is_a_correction_and_is_allowed(monkeypatch):
    # Only the history file changed: no compiled default can have changed here.
    kv = _kv([HISTORY], [{"namespace": "Session", "name": "no_such_setting_at_all"}])
    assert _run(monkeypatch, kv) == ""


def test_history_plus_unrelated_non_source_change_is_still_a_correction(monkeypatch):
    kv = _kv(
        [HISTORY, "docs/reference/settings/session-settings/overview.mdx", "ci/jobs/check_style.py"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    assert _run(monkeypatch, kv) == ""


def test_default_defined_by_a_constant_outside_the_declaration_files_is_enforced(
    monkeypatch,
):
    # src/Core/Defines.h holds constants such as `DEFAULT_INSERT_BLOCK_SIZE` that several
    # settings use as their default, so a change there must not bypass the current-block rule.
    kv = _kv(
        [HISTORY, "src/Core/Defines.h"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    error = _run(monkeypatch, kv)
    assert "no_such_setting_at_all" in error


def test_build_definition_config_template_is_enforced(monkeypatch):
    # Compile definitions such as `CLICKHOUSE_CLOUD` and `ENABLE_DISTRIBUTED_CACHE` select
    # settings defaults at build time; they are carried by config templates like
    # src/Common/config.h.in, so a change there must not bypass the current-block rule.
    kv = _kv(
        [HISTORY, "src/Common/config.h.in"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)


def test_build_definition_cmake_files_are_enforced(monkeypatch):
    for build_file in ("CMakeLists.txt", "src/CMakeLists.txt", "cmake/limit_jobs.cmake"):
        kv = _kv(
            [HISTORY, build_file],
            [{"namespace": "Session", "name": "no_such_setting_at_all"}],
        )
        assert "no_such_setting_at_all" in _run(monkeypatch, kv), build_file


def test_declaration_file_change_is_enforced(monkeypatch):
    kv = _kv(
        [HISTORY, "src/Core/Settings.cpp"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)


def test_deleting_the_last_record_is_not_an_escape_hatch(monkeypatch):
    # End-to-end with the diff parser: a change that reverts a compiled default cannot escape
    # the rule by deleting the row that recorded the original change instead of recording the
    # revert. 03999_stateless_settings_history would be satisfied by such a deletion (it only
    # compares the current default with the newest recorded value), so the style check has to
    # catch it. Deleting a phantom record remains possible - that change touches only the
    # history file, which the gate lets through (see above).
    file_lines = [
        '        addSettingsChanges(settings_changes_history, "26.7",',
        "        {",
        "        });",
    ]
    patch = (
        "@@ -1,3 +1,3 @@\n"
        " {\n"
        '-            {"no_such_setting_at_all", 0, 1, "Recorded in 26.7"},\n'
        " });\n"
    )
    changed = parse_settings_history_changes(patch, file_lines)
    assert changed == [{"namespace": "Session", "name": "no_such_setting_at_all"}]
    kv = _kv([HISTORY, "src/Core/Settings.cpp"], changed)
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)

    # The same deletion without any other source change is a historical correction: allowed.
    assert _run(monkeypatch, _kv([HISTORY], changed)) == ""


def test_moving_a_record_to_an_older_block_is_not_an_escape_hatch(monkeypatch):
    # The "move instead of delete" variant: the record is re-added verbatim under an older
    # version block. Nothing about the newest recorded value changes, so
    # 03999_stateless_settings_history passes, but `compatibility` would attribute the default
    # flip to the wrong release. The parser reports the setting once and the style check demands
    # it under the current version block.
    entry = '            {"no_such_setting_at_all", 0, 1, "Recorded here now"},'
    file_lines = [
        '        addSettingsChanges(settings_changes_history, "26.8",',
        "        {",
        "        });",
        '        addSettingsChanges(settings_changes_history, "26.7",',
        "        {",
        entry,
        "        });",
    ]
    patch = (
        "@@ -1,4 +1,3 @@\n"
        ' addSettingsChanges(settings_changes_history, "26.8",\n'
        " {\n"
        f"-{entry}\n"
        " });\n"
        "@@ -5,3 +4,4 @@\n"
        ' addSettingsChanges(settings_changes_history, "26.7",\n'
        " {\n"
        f"+{entry}\n"
        " });\n"
    )
    changed = parse_settings_history_changes(patch, file_lines)
    assert changed == [{"namespace": "Session", "name": "no_such_setting_at_all"}]
    kv = _kv([HISTORY, "src/Core/Settings.cpp"], changed)
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)


def test_no_history_change_at_all_is_skipped(monkeypatch):
    assert _run(monkeypatch, _kv(["src/Core/Settings.cpp"], [])) == ""


def test_editing_a_block_header_is_not_an_escape_hatch(monkeypatch):
    # The block-granularity variant: not a single entry line changes, only the version in the
    # `addSettingsChanges` header, which reassigns every record underneath to another release.
    # Without the block-level path the parser would report nothing and the style check would
    # treat the change as "nothing to validate" while `compatibility` starts serving the wrong
    # value for two releases at once.
    entry = '            {"no_such_setting_at_all", 0, 1, "Recorded here"},'
    file_lines = [
        '        addSettingsChanges(settings_changes_history, "26.7",',
        "        {",
        entry,
        "        });",
    ]
    patch = (
        "@@ -1,4 +1,4 @@\n"
        '-        addSettingsChanges(settings_changes_history, "26.8",\n'
        '+        addSettingsChanges(settings_changes_history, "26.7",\n'
        "         {\n"
        f" {entry}\n"
        "         });\n"
    )
    changed = parse_settings_history_changes(patch, file_lines)
    assert changed == [{"namespace": "Session", "name": "no_such_setting_at_all"}]
    kv = _kv([HISTORY, "src/Core/Settings.cpp"], changed)
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)

    # The same header edit without any other source change is a historical correction: allowed.
    assert _run(monkeypatch, _kv([HISTORY], changed)) == ""


def _rename_patch(old_name, new_name, block_version="26.8"):
    """A pure rename of one record inside one block: only the setting name differs."""
    reason = "New setting."
    added = f'            {{"{new_name}", false, false, "{reason}"}},'
    removed = f'            {{"{old_name}", false, false, "{reason}"}},'
    file_lines = [
        f'        addSettingsChanges(settings_changes_history, "{block_version}",',
        "        {",
        added,
        "        });",
    ]
    patch = (
        "@@ -1,4 +1,4 @@\n"
        f' addSettingsChanges(settings_changes_history, "{block_version}",\n'
        " {\n"
        f"-{removed}\n"
        f"+{added}\n"
        " });\n"
    )
    return patch, file_lines


def test_renaming_a_record_in_place_is_allowed(monkeypatch):
    # End-to-end with the diff parser: renaming a setting renames its record. The old name must
    # not be demanded under the current version block, because no history file could satisfy
    # that and 03999_stateless_settings_history at the same time - that test rejects a documented
    # name which is no longer a setting ("DOES NOT EXIST (typo/rename?)"), and for a MergeTree
    # setting not even an alias makes the old name reappear (system.merge_tree_settings has no
    # alias rows). `s3_base` is a real record of the current version block, so the check is
    # satisfied by the new name alone.
    patch, file_lines = _rename_patch("legacy_s3_base_name", "s3_base")
    changed = parse_settings_history_changes(patch, file_lines)
    assert changed == [{"namespace": "Session", "name": "s3_base"}]
    assert _run(monkeypatch, _kv([HISTORY, "src/Core/Settings.cpp"], changed)) == ""


def test_renaming_a_record_still_requires_the_new_name_under_the_current_block(monkeypatch):
    # The rename is not a free pass: the NEW name goes through the current-block rule as any
    # added record does, so renaming into a record that is not recorded under the current
    # version still fails.
    patch, file_lines = _rename_patch("some_old_name", "no_such_setting_at_all")
    changed = parse_settings_history_changes(patch, file_lines)
    assert changed == [{"namespace": "Session", "name": "no_such_setting_at_all"}]
    error = _run(monkeypatch, _kv([HISTORY, "src/Core/Settings.cpp"], changed))
    assert "no_such_setting_at_all" in error
