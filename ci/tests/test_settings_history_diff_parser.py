"""
Tests for `parse_settings_history_changes` in
`ci/jobs/scripts/workflow_hooks/store_data.py`, which feeds the
`settings_changes_history` style check with the settings a change adds to
`src/Core/SettingsChangesHistory.cpp`.

The parser reports new records, value changes (including an in-place edit of an existing entry),
removed records and records moved to another version or namespace block; only reason-only edits
of an entry that stays in its block are ignored. Whether such a change must sit under the current
version
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


def test_pure_removal_is_reported():
    # Dropping a record changes what the history says, so it is reported: otherwise a change
    # that reverts a compiled default to an older value could delete the row recording the
    # original change instead of recording the revert, and both the style check and
    # 03999_stateless_settings_history (which only compares the current default with the newest
    # recorded value) would stay green. Deleting a phantom record is still possible - the
    # source-file gate lets a change that touches only this file through.
    patch = (
        "@@ -9,3 +9,2 @@\n"
        " {\n"
        '-            {"old_setting", 1, 2, "Phantom record"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "Session", "name": "old_setting"}
    ]


def test_removal_namespace_comes_from_the_enclosing_block():
    patch = (
        "@@ -5,4 +5,3 @@\n"
        " {\n"
        '-            {"brand_new_merge_tree_setting", 0, 1, "New setting"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "MergeTree", "name": "brand_new_merge_tree_setting"}
    ]


def test_entry_moved_to_an_older_version_block_is_reported():
    # "Move instead of delete": the record is re-added verbatim under an older version block.
    # The newest recorded value is unchanged, so 03999_stateless_settings_history still passes,
    # but `compatibility` would attribute the default flip to the wrong release - so the parser
    # must report it once and let the style check demand it under the current version block.
    patch = (
        "@@ -1,4 +1,3 @@\n"
        " {\n"
        '-            {"old_setting", 0, 1000, "Fixed record"},\n'
        " });\n"
        "@@ -9,3 +8,4 @@\n"
        " {\n"
        '+            {"old_setting", 0, 1000, "Fixed record"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "Session", "name": "old_setting"}
    ]


def test_entry_moved_between_namespaces_is_reported():
    # The same version, but the record hops from the session history to the MergeTree history.
    patch = (
        "@@ -1,4 +1,3 @@\n"
        " {\n"
        '-            {"shared_name", 0, 1, "Moved"},\n'
        " });\n"
        "@@ -5,3 +4,4 @@\n"
        " {\n"
        '+            {"shared_name", 0, 1, "Moved"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "MergeTree", "name": "shared_name"},
        {"namespace": "Session", "name": "shared_name"},
    ]


def test_entry_reordered_inside_the_same_block_is_ignored():
    # Both sides sit in the same block, so nothing about what the history records changed.
    patch = (
        "@@ -1,4 +1,4 @@\n"
        " addSettingsChanges(settings_changes_history, \"26.8\",\n"
        " {\n"
        '-            {"brand_new_setting", false, true, "New setting"},\n'
        '             {"another_setting", 0, 1, "Untouched"},\n'
        '+            {"brand_new_setting", false, true, "New setting"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == []


def test_removal_and_replacement_with_another_value_reports_the_setting_once():
    # A revert recorded properly: the old row is dropped and a new row with a different value is
    # added. Both sides name the same setting in the same namespace, so it is reported once.
    patch = (
        "@@ -9,3 +9,3 @@\n"
        " {\n"
        '-            {"old_setting", 0, 1000, "Fixed record"},\n'
        '+            {"old_setting", 1000, 0, "Reverted"},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, FILE_LINES) == [
        {"namespace": "Session", "name": "old_setting"}
    ]


HEADER_EDIT_FILE_LINES = [
    "        addSettingsChanges(settings_changes_history, \"26.7\",",
    "        {",
    "            {\"first_setting\", false, true, \"Untouched\"},",
    "            {\"second_setting\", 0, 1, \"Untouched\"},",
    "        });",
    "        addSettingsChanges(settings_changes_history, \"26.6\",",
    "        {",
    "            {\"older_setting\", 0, 1, \"Untouched\"},",
    "        });",
]


def test_block_header_version_edit_reports_every_entry_of_the_block():
    # The block-granularity variant of "move instead of delete": no entry line changes at all,
    # only the version in the `addSettingsChanges` header, which reassigns every record
    # underneath to another release. An entry-only scan returns nothing and the style check
    # would skip, so `compatibility` could be made to attribute the flips to the wrong release.
    patch = (
        "@@ -1,5 +1,5 @@\n"
        '-        addSettingsChanges(settings_changes_history, "26.8",\n'
        '+        addSettingsChanges(settings_changes_history, "26.7",\n'
        "         {\n"
        '             {"first_setting", false, true, "Untouched"},\n'
        '             {"second_setting", 0, 1, "Untouched"},\n'
        "         });\n"
    )
    assert parse_settings_history_changes(patch, HEADER_EDIT_FILE_LINES) == [
        {"namespace": "Session", "name": "first_setting"},
        {"namespace": "Session", "name": "second_setting"},
    ]


def test_block_header_namespace_edit_reports_every_entry_of_the_block():
    # Same hole, the other axis: the whole block hops from the session history to the MergeTree
    # history without a single entry line changing.
    file_lines = [
        "        addSettingsChanges(merge_tree_settings_changes_history, \"26.8\",",
        "        {",
        "            {\"first_setting\", false, true, \"Untouched\"},",
        "        });",
    ]
    patch = (
        "@@ -1,4 +1,4 @@\n"
        '-        addSettingsChanges(settings_changes_history, "26.8",\n'
        '+        addSettingsChanges(merge_tree_settings_changes_history, "26.8",\n'
        "         {\n"
        '             {"first_setting", false, true, "Untouched"},\n'
        "         });\n"
    )
    assert parse_settings_history_changes(patch, file_lines) == [
        {"namespace": "MergeTree", "name": "first_setting"}
    ]


def test_block_header_edit_does_not_report_the_neighbouring_blocks():
    # Only the edited block is affected; the records of the block above and below keep saying
    # exactly what they said before.
    patch = (
        "@@ -6,4 +6,4 @@\n"
        '-        addSettingsChanges(settings_changes_history, "26.5",\n'
        '+        addSettingsChanges(settings_changes_history, "26.6",\n'
        "         {\n"
        '             {"older_setting", 0, 1, "Untouched"},\n'
        "         });\n"
    )
    assert parse_settings_history_changes(patch, HEADER_EDIT_FILE_LINES) == [
        {"namespace": "Session", "name": "older_setting"}
    ]


def test_a_new_block_reports_the_entries_it_introduces():
    # Opening the block for a new release: the header and its entries are added together. The
    # entries are reported, which is what the style check wants to see under the current block.
    patch = (
        "@@ -1,1 +1,5 @@\n"
        '+        addSettingsChanges(settings_changes_history, "26.7",\n'
        "+        {\n"
        '+            {"first_setting", false, true, "Untouched"},\n'
        '+            {"second_setting", 0, 1, "Untouched"},\n'
        "+        });\n"
        '         addSettingsChanges(settings_changes_history, "26.6",\n'
    )
    assert parse_settings_history_changes(patch, HEADER_EDIT_FILE_LINES) == [
        {"namespace": "Session", "name": "first_setting"},
        {"namespace": "Session", "name": "second_setting"},
    ]


def test_reason_only_edit_of_a_block_that_keeps_its_header_is_still_ignored():
    # The header line is untouched, so the block-level path must not fire and turn a harmless
    # reason-text edit into a violation.
    patch = (
        "@@ -1,5 +1,5 @@\n"
        '         addSettingsChanges(settings_changes_history, "26.7",\n'
        "         {\n"
        '-            {"first_setting", false, true, "Old wording"},\n'
        '+            {"first_setting", false, true, "Untouched"},\n'
        '             {"second_setting", 0, 1, "Untouched"},\n'
        "         });\n"
    )
    assert parse_settings_history_changes(patch, HEADER_EDIT_FILE_LINES) == []


RENAME_FILE_LINES = [
    "        addSettingsChanges(settings_changes_history, \"26.8\",",
    "        {",
    "            {\"new_name\", false, false, \"New setting.\"},",
    "        });",
    "        addSettingsChanges(settings_changes_history, \"26.7\",",
    "        {",
    "            {\"renamed_in_an_older_block\", false, false, \"New setting.\"},",
    "        });",
]


def test_pure_rename_in_place_reports_only_the_new_name():
    # Renaming a setting means renaming its record. The recorded values and the block stay the
    # same, so nothing is misattributed to another release - and the OLD name must not be
    # required under the current version block, because a record naming it cannot exist:
    # 03999_stateless_settings_history rejects a documented name that is no longer a setting.
    # The NEW name is reported, so the record still has to sit under the current version block.
    patch = (
        "@@ -1,4 +1,4 @@\n"
        " addSettingsChanges(settings_changes_history, \"26.8\",\n"
        " {\n"
        '-            {"old_name", false, false, "New setting."},\n'
        '+            {"new_name", false, false, "New setting."},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, RENAME_FILE_LINES) == [
        {"namespace": "Session", "name": "new_name"}
    ]


def test_rename_that_also_changes_the_recorded_values_reports_both_names():
    # Not a pure rename: the values differ, so this is a value change wearing a new name. Both
    # sides are reported and the current-block rule applies to each.
    patch = (
        "@@ -1,4 +1,4 @@\n"
        " addSettingsChanges(settings_changes_history, \"26.8\",\n"
        " {\n"
        '-            {"old_name", false, true, "New setting."},\n'
        '+            {"new_name", false, false, "New setting."},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, RENAME_FILE_LINES) == [
        {"namespace": "Session", "name": "new_name"},
        {"namespace": "Session", "name": "old_name"},
    ]


def test_unrelated_records_sharing_a_value_shape_are_both_reported():
    # `{"s", false, false, "New setting."}` is the most common record there is, so matching on
    # the values alone would read any deleted record plus any added record in the same block as
    # a rename of one another. The reason text must match too - here it does not, so the removal
    # is still reported and cannot be smuggled through behind an unrelated addition.
    patch = (
        "@@ -1,4 +1,4 @@\n"
        " addSettingsChanges(settings_changes_history, \"26.8\",\n"
        " {\n"
        '-            {"old_name", false, false, "Some other reason"},\n'
        '+            {"new_name", false, false, "New setting."},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, RENAME_FILE_LINES) == [
        {"namespace": "Session", "name": "new_name"},
        {"namespace": "Session", "name": "old_name"},
    ]


def test_rename_across_version_blocks_reports_both_names():
    # The record is renamed AND lands in a different block than the one it was removed from, so
    # the release it is attributed to changed. That is the misattribution the check exists for,
    # so both names are reported.
    patch = (
        "@@ -1,4 +1,3 @@\n"
        " addSettingsChanges(settings_changes_history, \"26.8\",\n"
        " {\n"
        '-            {"old_name", false, false, "New setting."},\n'
        " });\n"
        "@@ -5,3 +4,4 @@\n"
        " addSettingsChanges(settings_changes_history, \"26.7\",\n"
        " {\n"
        '+            {"renamed_in_an_older_block", false, false, "New setting."},\n'
        " });\n"
    )
    assert parse_settings_history_changes(patch, RENAME_FILE_LINES) == [
        {"namespace": "Session", "name": "renamed_in_an_older_block"},
        {"namespace": "Session", "name": "old_name"},
    ]
