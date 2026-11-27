"""
Regression test for the `clickhouse-test` suite-discovery race fixed in PR #109120.

Concurrent `clickhouse-test` invocations share one queries directory. When a run
needs engine substitutions, `SharedEngineReplacer` writes a transient per-PID copy
of a test file next to the original (see `SharedEngineReplacer.temp_file_path`) and
removes it once the test finishes. Another invocation's suite scan (`os.listdir` in
`get_selected_tests`) can observe that copy; because it ends in a supported
extension, `is_test_from_dir` used to accept it and schedule it as a real test — which
then failed either as `FileNotFoundError` once the copy vanished (in
`load_tags_and_random_settings_limits_from_file` / `is_valid_utf_8`) or as
`NO_REFERENCE` while the copy was still present.

The fix embeds a dedicated marker (`SharedEngineReplacer.TEMP_FILE_MARKER`) into these
copies and skips only marked files at the single discovery choke point
(`is_transient_test_copy` in `is_test_from_dir`). This test pins that producer/filter
contract:
  - the marked temp copy `foo.shared_engine_replacer_tmp.<pid>.sql` is skipped, and
  - a genuine test whose basename merely looks like `<name>.<digits><ext>` (e.g.
    `foo.1.sql`) is still discovered even when `foo.sql` also exists.

The second point guards against the earlier revision, which inferred "transient copy"
from the basename shape and silently dropped a legitimate `foo.1.sql` whenever
`foo.sql` was also present.
"""

import argparse
import os
import runpy
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")

# Load clickhouse-test without running __main__ so path changes propagate
# automatically. runpy.run_path handles the missing .py extension.
_ct = runpy.run_path(_CLICKHOUSE_TEST)

SharedEngineReplacer = _ct["SharedEngineReplacer"]
is_test_from_dir = _ct["is_test_from_dir"]
is_transient_test_copy = _ct["is_transient_test_copy"]
TEST_FILE_EXTENSIONS = _ct["TEST_FILE_EXTENSIONS"]


def _discovered(suite_dir):
    return {case for case in os.listdir(suite_dir) if is_test_from_dir(suite_dir, case)}


def test_transient_copy_skipped_but_lookalike_test_discovered():
    with tempfile.TemporaryDirectory() as suite_dir:
        original = os.path.join(suite_dir, "foo.sql")
        lookalike = os.path.join(suite_dir, "foo.1.sql")
        for path in (original, lookalike):
            with open(path, "w", encoding="utf-8") as f:
                f.write("SELECT 1;\n")

        # Only the attributes SharedEngineReplacer reads for a non-replicated,
        # non-cloud substitution; enough to make it materialize the temp copy.
        args = argparse.Namespace(
            replace_log_memory_with_mergetree=False,
            shared_catalog_stress=False,
        )

        # Materialize the transient per-PID copy exactly as a concurrent run would.
        with SharedEngineReplacer(
            args,
            original,
            False,  # replace_replicated
            True,  # replace_non_replicated
            False,  # reference_file
            False,  # cloud
        ) as replacer:
            temp_path = replacer.get_path()
            temp_case = os.path.basename(temp_path)

            # Precondition: a real, marked, supported-extension copy was created.
            assert temp_path != original, "SharedEngineReplacer did not create a copy"
            assert os.path.isfile(temp_path), temp_path
            assert (
                temp_case
                == f"foo.{SharedEngineReplacer.TEMP_FILE_MARKER}.{os.getpid()}.sql"
            ), temp_case
            assert any(temp_case.endswith(ext) for ext in TEST_FILE_EXTENSIONS)

            # The marked copy is recognized as transient and excluded from discovery.
            assert is_transient_test_copy(temp_case), temp_case
            assert not is_test_from_dir(suite_dir, temp_case), temp_case

            # Both originals — including the `foo.1.sql` look-alike the old shape
            # heuristic dropped — are discovered normally.
            assert not is_transient_test_copy("foo.sql")
            assert not is_transient_test_copy("foo.1.sql")
            assert is_test_from_dir(suite_dir, "foo.sql")
            assert is_test_from_dir(suite_dir, "foo.1.sql")

            # A whole-directory scan discovers exactly the two real tests and skips
            # only the marked copy.
            assert _discovered(suite_dir) == {"foo.sql", "foo.1.sql"}

        # The context manager removed the copy; discovery is unchanged.
        assert not os.path.isfile(temp_path)
        assert _discovered(suite_dir) == {"foo.sql", "foo.1.sql"}
