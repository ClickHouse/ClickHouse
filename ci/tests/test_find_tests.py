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
            "tests/queries/0_stateless/03999_stateless_settings_history.sh"
        )
        == "03999_stateless_settings_history"
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
            "tests/queries/0_stateless/03999_stateless_settings_history.reference"
        )
        == "03999_stateless_settings_history"
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


def test_format_schema_maps_to_owning_test_by_extensionless_stem():
    # Format schemas are conventionally referenced without the extension
    # (`format_schema = '00825_protobuf_format_persons:Person'`), so the literal
    # filename never appears in any test body. The stem fallback still maps the
    # fixture to exactly its owning test instead of every `00825_*` sibling.
    owning = Targeting._tests_owning_data_file(
        "tests/queries/0_stateless/format_schemas/00825_protobuf_format_persons.proto"
    )
    assert owning == ["00825_protobuf_format_persons"]


def test_literal_filename_match_beats_short_stem():
    # `03250.proto` is referenced by filename in exactly one test, but its bare
    # stem `03250` also appears in an unrelated sibling's body (tests routinely
    # embed their own numeric prefix in table names). The literal match must
    # take precedence, or the short stem would broaden a mapping that was
    # already precise.
    owning = Targeting._tests_owning_data_file(
        "tests/queries/0_stateless/format_schemas/03250.proto"
    )
    assert owning == ["03250_SYSTEM_DROP_FORMAT_SCHEMA_CACHE_FOR_Protobuf"]


def test_literal_filename_match_beats_cross_extension_stem():
    # `03036_archive1.tar` is read by one test, while a sibling reads only
    # `03036_archive1.zip`. The shared stem `03036_archive1` must not pull the
    # `.zip`-only sibling into the `.tar` fixture's mapping.
    owning = Targeting._tests_owning_data_file(
        "tests/queries/0_stateless/data_minio/03036_archive1.tar"
    )
    assert owning == ["03036_reading_s3_archives"]


def test_orphan_data_file_maps_to_owning_test_by_prefix():
    # PR #104097 reproducer: `_derive_test_name` returns None for a `.tsv`, so it
    # used to be skipped. The frozen baseline consumed by the settings history test
    # carries that test's `03999` prefix and is referenced by its literal filename,
    # so the flaky check reruns exactly that test.
    owning = Targeting._tests_owning_data_file(
        "tests/queries/0_stateless/03999_settings_history_baseline.tsv"
    )
    assert owning == ["03999_stateless_settings_history"]


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


def _targeting_with_job_type(job_type):
    targeter = Targeting.__new__(Targeting)
    targeter.job_type = job_type
    return targeter


def test_existing_stateless_test_still_exists():
    targeter = _targeting_with_job_type(Targeting.STATELESS_JOB_TYPE)
    assert targeter._test_exists("00001_select_1")


def test_deleted_stateless_test_no_longer_exists():
    # PR #110958 reproducer: `04648_geohashes_in_box_cancellation` failed on
    # the PR, was then deleted from master as flaky, and the targeted job kept
    # replaying its name from CIDB — clickhouse-test matched zero tests and
    # exited 1 with "No tests were run.".
    targeter = _targeting_with_job_type(Targeting.STATELESS_JOB_TYPE)
    assert not targeter._test_exists("04648_geohashes_in_box_cancellation")


def test_existing_integration_test_still_exists():
    targeter = _targeting_with_job_type(Targeting.INTEGRATION_JOB_TYPE)
    assert targeter._test_exists("test_storage_s3/test.py::test_case[param]")
    assert targeter._test_exists("test_storage_s3")


def test_deleted_integration_test_no_longer_exists():
    targeter = _targeting_with_job_type(Targeting.INTEGRATION_JOB_TYPE)
    assert not targeter._test_exists("test_no_such_directory/test.py::test_case")
