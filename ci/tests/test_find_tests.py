"""
Regression tests for `ci/jobs/scripts/find_tests.py` test-name derivation.

PR #104097 changed only `tests/queries/0_stateless/02995_settings_26_4_1.tsv`
under `tests/queries/0_stateless/`.  The flaky-check driver derived the test
name `02995_settings_26_4_1` by stripping the extension and asked
`clickhouse-test` to re-run it 50 times — but no test with that base name
exists (the `.tsv` is a data file consumed by `02995_new_settings_history.sh`).
The filter matched zero tests and `clickhouse-test` exited with code 1.

These tests pin the corrected behaviour: orphan supporting files are skipped,
and supporting files with a real sibling test (e.g. `.reference`) still map
back to that test.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.find_tests import Targeting


def test_orphan_data_file_is_skipped():
    # PR #104097 reproducer: a `.tsv` data file consumed by another test.
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/02995_settings_26_4_1.tsv"
        )
        is None
    )


def test_test_source_files_keep_base_name():
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/02995_new_settings_history.sh"
        )
        == "02995_new_settings_history"
    )
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/02995_index_1.sql"
        )
        == "02995_index_1"
    )
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/00172_hits_joins.sql.j2"
        )
        == "00172_hits_joins"
    )


def test_reference_file_maps_to_sibling_test():
    # `.reference` for a sibling `.sh`.
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/02995_new_settings_history.reference"
        )
        == "02995_new_settings_history"
    )
    # `.reference.j2` for a sibling `.sql.j2`.
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/00172_hits_joins.reference.j2"
        )
        == "00172_hits_joins"
    )


def test_unknown_data_file_with_no_sibling_is_skipped():
    assert (
        Targeting._derive_test_name(
            "tests/queries/0_stateless/99999_no_such_test.tsv"
        )
        is None
    )
