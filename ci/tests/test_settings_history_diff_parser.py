"""
Tests for `parse_settings_history_changes` in
`ci/jobs/scripts/workflow_hooks/store_data.py`, which feeds the
`settings_changes_history` style check with the settings a change adds to
`src/Core/SettingsChangesHistory.cpp`.

The parser reports new records and value changes (including an in-place edit of an existing
entry), but not reason-only edits. Whether such a change must sit under the current version
block is decided by the style check, not the parser: it enforces the rule as soon as any other
C++ source file changed, so an edit that touches only SettingsChangesHistory.cpp - a historical
correction - is allowed there. The parser therefore reports in-place value edits; the source-file
gate in check_settings_changes_history is what distinguishes a real default change from a
correction (see ci/tests/test_settings_history_source_gate.py).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py`, imported by `store_data`, imports `praktika` as a top-level module.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks.store_data import parse_settings_history_changes


FILE_LINES = [
    "        addSettingsChanges(settings_changes_history, \"26.8\",",
    "        {",
    "            {\"brand_new_setting\", false, true, \"New setting\"},",
    "        });",
    "        addSettingsChanges(merge_tree_settings_changes_history, \"26.8\",",
    "        {",
    "            {\"brand_new_merge_tree_setting\", 0, 1, \"New setting\"},",
    "        });",
    "        addSettingsChanges(settings_changes_history, \"24.2\",",
    "        {",
    "            {\"old_setting\", 0, 1000, \"Fixed record\"},",
    "        });",
]


def test_new_entry_is_reported():
    patch = "@@ -1,2 +1,3 @@\n" " {\n" '+            {"brand_new_setting", false, true, "New setting"},\n' " });\n"
    # The added line lands on new-file line 2, inside the 26.8 session block.
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "Session", "name": "brand_new_setting"}
    ]


def test_namespace_comes_from_the_enclosing_block():
    patch = (
        "@@ -5,2 +5,3 @@\n"
        " {\n"
        '+            {"brand_new_merge_tree_setting", 0, 1, "New setting"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "MergeTree", "name": "brand_new_merge_tree_setting"}
    ]


def test_reason_only_edit_is_ignored():
    patch = (
        "@@ -1,3 +1,3 @@\n"
        " {\n"
        '-            {"brand_new_setting", false, true, "Old reason"},\n'
        '+            {"brand_new_setting", false, true, "New setting"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == []


def test_in_place_value_edit_is_reported():
    # An in-place value edit of an existing entry (value-signature differs) is reported by the
    # parser. Whether it is required under the current version block is up to the style check:
    # if only SettingsChangesHistory.cpp changed it is a historical correction and is allowed,
    # but if a settings source also changed it is a real default change and must be current.
    patch = (
        "@@ -9,3 +9,3 @@\n"
        " {\n"
        '-            {"old_setting", 0, 0, "Fixed record"},\n'
        '+            {"old_setting", 0, 1000, "Fixed record"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "Session", "name": "old_setting"}
    ]


def test_pure_removal_is_ignored():
    patch = (
        "@@ -9,3 +9,2 @@\n"
        " {\n"
        '-            {"old_setting", 1, 2, "Phantom record"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == []
