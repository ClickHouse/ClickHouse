"""
Tests for `parse_settings_history_changes` in
`ci/jobs/scripts/workflow_hooks/store_data.py`, which feeds the
`settings_changes_history` style check with the settings a change adds to
`src/Core/SettingsChangesHistory.cpp`.

The parser must report genuinely new records (they have to be listed under the current
version block), but not reason-only edits and not in-place corrections of an already
recorded entry: fixing what a past release actually did is not a default change made by
the pull request, and recording it under the current version would tell `compatibility`
that the value changed again in this release, which never happened.
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


def test_in_place_correction_of_a_historical_entry_is_ignored():
    # PR #111841: the recorded `new_value` of a past release was wrong and is corrected in
    # the block of the version where the change actually happened. Requiring an entry under
    # the current version block would record a default change that never happened.
    patch = (
        "@@ -9,3 +9,3 @@\n"
        " {\n"
        '-            {"old_setting", 0, 0, "Fixed record"},\n'
        '+            {"old_setting", 0, 1000, "Fixed record"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == []


def test_pure_removal_is_ignored():
    patch = (
        "@@ -9,3 +9,2 @@\n"
        " {\n"
        '-            {"old_setting", 1, 2, "Phantom record"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == []
