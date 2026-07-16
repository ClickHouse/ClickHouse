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
import types

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


def test_subdirectory_data_fixture_maps_to_owning_test():
    # A merge-queue drift guard must not false-green when a PR changes only a
    # data fixture nested in a subdirectory. `data_parquet/02716_data.parquet`
    # is consumed by `02716_parquet_invalid_date32.sh`; the prefix-narrowed
    # content scan reruns exactly that test.
    owning = Targeting._tests_owning_data_file(
        "tests/queries/0_stateless/data_parquet/02716_data.parquet"
    )
    assert owning == ["02716_parquet_invalid_date32"]


def test_orphan_data_file_maps_to_owning_test_by_prefix():
    # PR #104097 reproducer: `_derive_test_name` returns None for this `.tsv`,
    # so it used to be skipped. It carries the `02995` prefix of the test that
    # consumes it, so the flaky check now reruns that test instead of skipping.
    owning = Targeting._tests_owning_data_file(
        "tests/queries/0_stateless/02995_settings_26_4_1.tsv"
    )
    assert "02995_new_settings_history" in owning


def test_data_file_with_no_matching_test_is_skipped():
    # No test with this prefix exists: emit nothing rather than a pattern that
    # matches no test (which would make clickhouse-test exit 1).
    assert (
        Targeting._tests_owning_data_file(
            "tests/queries/0_stateless/data_parquet/99999_no_such_test.parquet"
        )
        == []
    )


def test_data_file_without_numeric_prefix_is_skipped():
    # A shared fixture with no owning-test prefix cannot be mapped.
    assert (
        Targeting._tests_owning_data_file(
            "tests/queries/0_stateless/data_parquet/shared_data.parquet"
        )
        == []
    )


def _targeting_from_diff(diff_text):
    info = types.SimpleNamespace(
        job_name="Stateless tests (flaky check)", is_local_run=False
    )
    targeter = Targeting(info=info)
    targeter._diff_text = diff_text
    return targeter


def test_get_changed_tests_reruns_owning_test_for_fixture_only_diff():
    # End-to-end reproducer for the merge-queue drift-guard gap: a PR whose only
    # stateless change is a nested data fixture must still select the test that
    # consumes it, so neither the config-time skip nor the in-job selection can
    # false-green the merge queue.
    diff = "+++ b/tests/queries/0_stateless/data_parquet/02716_data.parquet\n"
    assert _targeting_from_diff(diff).get_changed_tests() == [
        "02716_parquet_invalid_date32."
    ]


def test_get_changed_tests_skips_unmappable_fixture():
    # A fixture that maps to no real test yields no pattern (clickhouse-test
    # would exit 1 on a zero-match run).
    diff = "+++ b/tests/queries/0_stateless/data_parquet/99999_no_such_test.parquet\n"
    assert _targeting_from_diff(diff).get_changed_tests() == []
