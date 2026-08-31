"""
Tests for the removed-setting exemption of the `settings_changes_history` style check in
`ci/jobs/check_style.py`.

A setting that a change deletes from the code cannot be recorded in
`src/Core/SettingsChangesHistory.cpp` at all: `03999_stateless_settings_history` rejects a record
naming a setting that is not in `system.settings` / `system.merge_tree_settings`, and
`applyCompatibilitySetting` resolves every recorded name. So its records must be deleted together
with it, and the check must not demand an entry under the current version block - otherwise a
setting that was never released could not be dropped at all. A setting that merely becomes
OBSOLETE keeps its row in `system.settings`, so its records stay required.
"""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `praktika` is imported as a top-level module by the style-check job.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs import check_style
from ci.jobs.scripts.workflow_hooks.store_data import parse_settings_history_changes


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
HISTORY = "src/Core/SettingsChangesHistory.cpp"
# The change removes the setting, so it necessarily touches the declarations too - which is what
# makes the source-file gate enforce the current-version-block rule in the first place.
CHANGED_FILES = [HISTORY, "src/Core/Settings.cpp"]


class _FakeInfo:
    def __init__(self, kv):
        self._kv = kv

    def __call__(self):
        return self

    def get_kv_data(self):
        return self._kv


def _run(monkeypatch, changed_settings, changed_files=None):
    kv = {
        "changed_files": changed_files if changed_files is not None else CHANGED_FILES,
        "settings_history_changed_settings": changed_settings,
    }
    monkeypatch.setattr(check_style, "Info", _FakeInfo(kv))
    monkeypatch.chdir(REPO_ROOT)
    return check_style.check_settings_changes_history()


def _removal_of(name, block='addSettingsChanges(settings_changes_history, "26.7",'):
    """The parser's view of a change that deletes the only record of `name` from `block`."""
    file_lines = ["        " + block, "        {", "        });"]
    patch = (
        "@@ -1,3 +1,3 @@\n"
        " {\n"
        f'-            {{"{name}", 0, 1, "Recorded in an older release"}},\n'
        " });\n"
    )
    return parse_settings_history_changes(patch, file_lines)


def test_removing_a_setting_that_no_longer_exists_is_allowed(monkeypatch):
    # The setting is gone from the declarations, so its record cannot be kept anywhere: the
    # deletion is the only correct thing to do and must not be reported.
    changed = _removal_of("no_such_setting_at_all")
    assert changed == [{"namespace": "Session", "name": "no_such_setting_at_all"}]
    assert _run(monkeypatch, changed) == ""


def test_removing_a_merge_tree_setting_that_no_longer_exists_is_allowed(monkeypatch):
    changed = _removal_of(
        "no_such_setting_at_all",
        'addSettingsChanges(merge_tree_settings_changes_history, "26.7",',
    )
    assert changed == [{"namespace": "MergeTree", "name": "no_such_setting_at_all"}]
    assert _run(monkeypatch, changed) == ""


def test_removing_the_record_of_a_setting_that_stays_is_still_reported(monkeypatch):
    # The setting is still declared, so deleting its record is the escape hatch the check exists
    # to catch: the revert has to be recorded under the current version block instead.
    changed = _removal_of("max_block_size")
    assert changed == [{"namespace": "Session", "name": "max_block_size"}]
    assert "max_block_size" in _run(monkeypatch, changed)


def test_removing_the_record_of_a_merge_tree_setting_that_stays_is_still_reported(
    monkeypatch,
):
    changed = _removal_of(
        "index_granularity",
        'addSettingsChanges(merge_tree_settings_changes_history, "26.7",',
    )
    assert changed == [{"namespace": "MergeTree", "name": "index_granularity"}]
    assert "index_granularity" in _run(monkeypatch, changed)


def test_an_obsolete_setting_is_not_a_removed_setting(monkeypatch):
    # `MAKE_OBSOLETE` keeps the row in system.settings, so the records of such a setting are
    # still resolvable and still required - making a setting obsolete is not a removal.
    declared = check_style.declared_setting_names()[0]
    obsolete = "allow_experimental_query_deduplication"
    assert obsolete in declared["Session"], "expected an obsolete setting to count as declared"
    assert obsolete in _run(
        monkeypatch, [{"namespace": "Session", "name": obsolete}]
    )


def test_an_alias_counts_as_a_declared_setting(monkeypatch):
    # History records may name an alias (`applyCompatibilitySetting` resolves them), and an alias
    # has its own row in system.settings, so it must not be mistaken for a removed setting.
    declared = check_style.declared_setting_names()[0]
    assert "insert_distributed_sync" in declared["Session"]
    assert "insert_distributed_timeout" in declared["Session"]


def test_a_setting_of_the_other_namespace_does_not_count_as_declared(monkeypatch):
    # The namespaces are resolved separately: an overlapping name that only exists as a MergeTree
    # setting must not make a removed Session setting look declared.
    declared = check_style.declared_setting_names()[0]
    assert "index_granularity" in declared["MergeTree"]
    assert "index_granularity" not in declared["Session"]


def test_unrecognized_namespace_is_not_exempt(monkeypatch):
    # Fail-close: a namespace that cannot be resolved to declarations is reported, not skipped.
    assert "made_up_setting" in _run(
        monkeypatch, [{"namespace": "NoSuchNamespace", "name": "made_up_setting"}]
    )


def test_every_recorded_setting_resolves_to_a_declaration(monkeypatch):
    """Rot detector for the declaration parser: `03999_stateless_settings_history` rejects a
    record that names a setting which does not exist, so on master every name recorded in
    SettingsChangesHistory.cpp must be found by `declared_setting_names`. If this fails, the
    parser (or `_SETTINGS_DECLARATION_SOURCES`) went stale and started exempting real settings
    from the current-version-block rule."""
    monkeypatch.chdir(REPO_ROOT)
    declared, error = check_style.declared_setting_names()
    assert error == "" and declared

    namespace_by_map = {
        "settings_changes_history": "Session",
        "merge_tree_settings_changes_history": "MergeTree",
    }
    block_re = re.compile(r'addSettingsChanges\(\s*(\w+)\s*,\s*"([\d.]+)"')
    entry_re = re.compile(r'^\s*\{\s*"([A-Za-z0-9_]+)"')
    namespace = None
    dangling = []
    recorded = 0
    with open(HISTORY, "r", encoding="utf-8") as f:
        for line in f:
            mb = block_re.search(line)
            if mb:
                namespace = namespace_by_map.get(mb.group(1))
                continue
            me = entry_re.match(line)
            if me and namespace:
                recorded += 1
                if me.group(1) not in declared[namespace]:
                    dangling.append(f"{namespace}: {me.group(1)}")
    assert recorded > 100, "the history file was not parsed"
    assert not dangling, "recorded settings not found in the declarations: " + ", ".join(
        sorted(set(dangling))
    )


def test_missing_declaration_file_fails_closed(monkeypatch):
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(
        check_style,
        "_SETTINGS_DECLARATION_SOURCES",
        {"Session": ("src/Core/NoSuchSettingsFile.cpp",)},
    )
    declared, error = check_style.declared_setting_names()
    assert declared is None
    assert "src/Core/NoSuchSettingsFile.cpp" in error


def test_declaration_file_without_any_setting_fails_closed(monkeypatch):
    # The file exists but no declaration matches: the macros moved or changed shape. Returning an
    # empty set would exempt every setting, so this is an error instead.
    monkeypatch.chdir(REPO_ROOT)
    monkeypatch.setattr(
        check_style,
        "_SETTINGS_DECLARATION_SOURCES",
        {"Session": ("src/Core/SettingsChangesHistory.cpp",)},
    )
    declared, error = check_style.declared_setting_names()
    assert declared is None
    assert "no Session setting declaration was found" in error


def test_removal_reported_by_a_block_header_edit_is_filtered_too(monkeypatch):
    # A block header edit reports every entry of the block; the ones whose setting is gone are
    # filtered out the same way, and a header edit that only covers such entries is allowed.
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
    assert _run(monkeypatch, changed) == ""
