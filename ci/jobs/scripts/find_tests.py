import argparse
import json
import os
import re
import sys
from pathlib import Path
from dataclasses import asdict
from datetime import datetime, timezone
import time

sys.path.append("./")

from ci.jobs.scripts.coverage_selection import (
    build_candidate_query,
    canonical_coverage_path,
    parse_rows,
    protect_selection,
    rank_candidates,
    snapshot_predicate,
    snapshot_query,
    validate_snapshots,
)
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG

from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell
from ci.settings.settings import SECRET_CI_DB_CONNECTION

# Query to fetch failed tests from CIDB for a given PR.
# Pre-filters out commit/check_name combinations with >= 20 failures — these indicate
# widespread failures (e.g. build broken, environment issue) where every test failed,
# not genuine per-test flakiness.  Results are ordered by recency-weighted failure
# count (exponential decay, 7-day half-life). Mandatory failures are not capped before final selection.
FAILED_TESTS_QUERY = """ \
 select test_name
 from checks
 where pull_request_number = {PR_NUMBER}
   and check_name LIKE '{JOB_TYPE}%'
   and check_status = 'failure'
   and match(test_name, '{TEST_NAME_PATTERN}')
   and test_status = 'FAIL'
   and check_start_time >= now() - interval 30 day
   and (commit_sha, check_name) in (
       select commit_sha, check_name
       from checks
       where pull_request_number = {PR_NUMBER}
         and check_name LIKE '{JOB_TYPE}%'
         and check_status = 'failure'
         and test_status = 'FAIL'
         and check_start_time >= now() - interval 30 day
       group by commit_sha, check_name
       having count(test_name) < 20
   )
 group by test_name
 order by count() * exp(-dateDiff('day', max(check_start_time), now()) / 7.) desc
 \
"""


class Targeting:
    INTEGRATION_JOB_TYPE = "Integration"
    STATELESS_JOB_TYPE = "Stateless"

    # The selected-test sanitizer jobs replace their full-suite counterparts,
    # so a change to the harness itself must not leave every one of those jobs
    # with an empty selection. Keep one inexpensive test for each of the
    # parallel and sequential flavors.
    STATELESS_HARNESS_SMOKE_TESTS = (
        "00001_select_1.",
        "01109_exchange_tables.",
    )

    # Feature-specific jobs also need to exercise the option that differentiates
    # them from the ordinary parallel or sequential lanes. Each pair contains a
    # parallel-safe test followed by a `no-parallel` test; flavor filtering below
    # retains the one suitable for the current lane.
    STATELESS_HARNESS_FEATURE_SMOKE_TESTS = {
        "s3 storage": (
            "03741_s3_glob_table_path_pushdown.",
            "02302_s3_file_pruning.",
        ),
        "distributed plan": (
            "04367_distributed_plan_merge_scatter_multishard.",
            "04648_distributed_plan_task_error_propagation.",
        ),
    }

    # Keep this in sync with the functional-test runner inputs in
    # `common_ft_job_config` and with the selected-test orchestration. A
    # change here means test selection or the runner configuration changed,
    # rather than a query test that can be discovered from its path.
    _STATELESS_HARNESS_PATHS = (
        ".github/workflows/pull_request.yml",
        "ci/defs/job_configs.py",
        "ci/jobs/functional_tests.py",
        "ci/jobs/scripts/clickhouse_proc.py",
        "ci/jobs/scripts/find_tests.py",
        "ci/jobs/select_functional_tests.py",
        "ci/jobs/scripts/coverage_selection.py",
        "ci/jobs/scripts/test_selection_config.py",
        "ci/jobs/scripts/test_selection_manifest.py",
        "ci/defs/functional_test_selection.py",
        "ci/jobs/scripts/functional_tests_results.py",
        "ci/jobs/scripts/log_export.py",
        "ci/jobs/scripts/workflow_hooks/filter_job.py",
        "ci/jobs/scripts/workflow_hooks/store_data.py",
        "ci/jobs/scripts/server_cleanup.py",
        "ci/praktika/info.py",
        "ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
        "ci/jobs/scripts/functional_tests/setup_seaweedfs.sh",
        "ci/praktika/cidb.py",
        "ci/workflows/pull_request.py",
        "tests/clickhouse-test",
    )
    _STATELESS_HARNESS_PATH_PREFIXES = (
        "ci/docker/stateless-test/",
        "tests/config/",
    )

    def __init__(self, info: Info):
        self.info = info
        self._cidb = None
        self.config = SELECTION_CONFIG
        self.selection_diagnostics = {}
        if "stateless" in info.job_name.lower():
            self.job_type = self.STATELESS_JOB_TYPE
        elif "integration" in info.job_name.lower():
            self.job_type = self.INTEGRATION_JOB_TYPE
        else:
            self.job_type = None

    def _ci_db(self):
        # Queries run as the privileged CI user. The public `play` user is
        # rate/row/time-limited and must be used only for links handed to humans
        # (see CIDB.get_link_to_test_case_statistics).
        if self._cidb is None:
            conn = json.loads(self.info.get_secret(SECRET_CI_DB_CONNECTION).get_value())
            self._cidb = CIDB(
                url=conn.get("url"),
                user=conn.get("user"),
                passwd=conn.get("password"),
            )
        return self._cidb

    # Keep in sync with TEST_FILE_EXTENSIONS in tests/clickhouse-test.
    _TEST_FILE_EXTENSIONS = (".sql.j2", ".sql", ".sh", ".py", ".expect")

    @classmethod
    def _derive_test_name(cls, fpath: str):
        """Map a changed file under `tests/queries/0_stateless/` to a test name.

        Returns the test base name (without extension) suitable for `clickhouse-test --test`,
        or `None` if the file does not correspond to a real test (e.g. a data file like
        `02995_settings_26_4_1.tsv`, which is consumed by `02995_new_settings_history.sh`
        but has no test of its own).
        """
        fname = os.path.basename(fpath)

        # Direct hit: the changed file is itself a test source file.
        for ext in cls._TEST_FILE_EXTENSIONS:
            if fname.endswith(ext):
                return fname[: -len(ext)]

        # Supporting file (`.reference`, `.reference.j2`, `.tsv`, ...). Walk the
        # extensions one at a time looking for a sibling test source file with
        # the same base name. This catches reference updates like
        # `00172_hits_joins.reference.j2` → `00172_hits_joins.sql.j2` while
        # rejecting orphan data files like `02995_settings_26_4_1.tsv`.
        test_dir = Path("tests/queries/0_stateless")
        candidate = fname
        while "." in candidate:
            candidate = candidate.rsplit(".", 1)[0]
            for ext in cls._TEST_FILE_EXTENSIONS:
                if (test_dir / f"{candidate}{ext}").is_file():
                    return candidate
        return None

    @classmethod
    def _tests_owning_data_file(cls, fpath: str):
        """Map an auxiliary stateless file back to the base names of the tests
        that own it.

        A data fixture may sit next to its test (`02995_settings_26_4_1.tsv`) or
        in a subdirectory (`data_parquet/02716_data.parquet`); either way it has
        no test of its own, so `_derive_test_name` skips it. But such a fixture
        change still alters the surface of the test that consumes it, and a drift
        guard that skips it would false-green the very drift it exists to catch.

        Fixtures carry their owning test's five-digit prefix by convention, so
        the prefix narrows the candidates to the `NNNNN_*` tests at the suite
        root (a handful of files, never the whole suite). Among those, prefer the
        ones whose body references the fixture's literal filename. When none
        does, retry with the extensionless stem, since format schemas are
        conventionally referenced without the extension
        (`format_schema = 'NNNNN_foo:Message'` for `format_schemas/NNNNN_foo.proto`).
        The stem is strictly a fallback: a short or cross-extension stem
        (`03250.proto` → `03250`, or `03036_archive1.tar` next to a test that
        reads `03036_archive1.zip`) would otherwise pull unrelated prefix
        siblings into mappings the literal filename already resolves precisely.
        Fall back to all prefix siblings when neither matches — the reference is
        then constructed dynamically and cannot be found textually. Return an
        empty list when the file has no numeric prefix or no test with that
        prefix exists — there is then genuinely nothing to rerun, and emitting a
        pattern that matches no test would make `clickhouse-test` exit 1 (the
        failure mode `PR #104097` fixed).
        """
        match = re.match(r"(\d{5})", os.path.basename(fpath))
        if match is None:
            return []
        prefix = match.group(1)
        test_dir = Path("tests/queries/0_stateless")
        candidates = {}
        for ext in cls._TEST_FILE_EXTENSIONS:
            for test_file in test_dir.glob(f"{prefix}_*{ext}"):
                candidates[test_file.name[: -len(ext)]] = test_file
        if not candidates:
            return []
        fname = os.path.basename(fpath)
        stem = os.path.splitext(fname)[0]
        by_fname = []
        by_stem = []
        for base_name, test_file in candidates.items():
            try:
                with test_file.open("r", encoding="utf-8", errors="ignore") as f:
                    body = f.read()
            except OSError:
                continue
            if fname in body:
                by_fname.append(base_name)
            elif stem in body:
                by_stem.append(base_name)
        referencing = by_fname or by_stem
        return sorted(referencing) if referencing else sorted(candidates)

    @staticmethod
    def is_functional_test_file(fpath: str) -> bool:
        """A changed path that is itself a stateless functional test source file."""
        fpath = fpath.removeprefix("./")
        return fpath.startswith("tests/queries/0_stateless/") and Path(fpath).is_file()

    @staticmethod
    def is_integration_test_file(fpath: str) -> bool:
        """A changed path that is itself an integration test module."""
        fpath = fpath.removeprefix("./")
        return (
            fpath.startswith("tests/integration/test_")
            and not fpath.startswith("tests/integration/test_e2e_")
            and Path(fpath).name.startswith("test")
            and fpath.endswith(".py")
            and Path(fpath).is_file()
        )

    @staticmethod
    def is_ci_job_script(fpath: str) -> bool:
        """A changed path under the CI job scripts themselves.

        Tolerated alongside test-file changes by the batch-skip check in
        `functional_tests.py` / `integration_test_job.py`: such a change is
        exercised identically by every batch, so a batch that does not
        contain the changed test file is still a valid check of the
        (possibly changed) job script.
        """
        fpath = fpath.removeprefix("./")
        return fpath.startswith("ci/jobs/") and Path(fpath).is_file()

    @classmethod
    def functional_test_hash_batch_file(cls, fpath: str):
        """Return the on-disk stateless test filename (with extension) that
        `clickhouse-test --run-by-hash-*` uses to bucket the given changed path,
        or `None` if it cannot be resolved to a concrete test source file.

        `clickhouse-test`'s `is_test_from_dir`/`get_selected_tests` only look
        at files directly inside the suite root (`os.listdir`, not
        recursive), so a file nested in a subdirectory - e.g.
        `tests/queries/0_stateless/helpers/httpclient.py` or
        `tests/queries/0_stateless/data_avro/generate_avro.sh` - is never a
        test case there, no matter its extension. Hashing such a nested
        file's basename would fabricate a bucket assignment that does not
        correspond to how `--run-by-hash-*` actually splits the suite, so
        return `None` (the caller then conservatively runs the batch) unless
        the file's parent directory is exactly the suite root.
        """
        test_dir = Path("tests/queries/0_stateless")
        path = Path(fpath.removeprefix("./"))
        if path.parent != test_dir:
            return None
        fname = path.name
        for ext in cls._TEST_FILE_EXTENSIONS:
            if fname.endswith(ext):
                return fname
        base_name = cls._derive_test_name(fpath)
        if base_name is None:
            return None
        for ext in cls._TEST_FILE_EXTENSIONS:
            if (test_dir / f"{base_name}{ext}").is_file():
                return f"{base_name}{ext}"
        return None

    @classmethod
    def functional_test_source_file(cls, test_name: str):
        """Return the stateless test source file name (e.g. `00001_x.sql`) for a
        test name as used by `clickhouse-test` (`00001_x` or `00001_x.`), or
        `None` when no such file exists in `tests/queries/0_stateless` - a
        stateful test, or a test that has since been removed or renamed.

        `clickhouse-test` reports rendered `.sql.j2` tests as `<name>.gen` in
        failures and as `<name>.gen.sql` in coverage. Normalize those report
        names to the template base before looking up the source file.
        """
        base_name = cls.normalize_stateless_test_name(test_name).rstrip(".")
        test_dir = Path("tests/queries/0_stateless")
        for ext in cls._TEST_FILE_EXTENSIONS:
            if (test_dir / f"{base_name}{ext}").is_file():
                return f"{base_name}{ext}"
        return None

    @staticmethod
    def normalize_stateless_test_name(test_name: str) -> str:
        """Map a rendered `.sql.j2` test name reported by CI to its source name.

        Rendered template tests are recorded as `<name>.gen` by test failures
        and `<name>.gen.sql` by per-test coverage, whereas `clickhouse-test`
        selectors match source test names. Keep a trailing dot, used for exact
        matching of changed tests, intact.
        """
        exact = test_name.endswith(".")
        base_name = test_name.rstrip(".")
        if base_name.endswith(".gen.sql"):
            base_name = base_name.removesuffix(".gen.sql")
        elif base_name.endswith(".gen"):
            base_name = base_name.removesuffix(".gen")
        return f"{base_name}." if exact else base_name

    @staticmethod
    def is_sequential_functional_test(test_source_file: str) -> bool:
        """True if the on-disk stateless test file (e.g. `00001_x.sql`, as
        returned by `functional_test_hash_batch_file`) is tagged `no-parallel`
        or `sequential`.

        Mirrors `clickhouse-test`'s own `is_sequential_test`/tag-parsing logic,
        so the batch-skip check in `functional_tests.py` can tell whether a
        changed test would even be selected by a job invoked with the
        `parallel`/`sequential` runner option (`--no-sequential`/`--no-parallel`),
        which splits the suite into two independently hash-batched job flavors.
        """
        if test_source_file.endswith(".sql") or test_source_file.endswith(".sql.j2"):
            comment_sign = "--"
        elif test_source_file.endswith((".sh", ".py", ".expect")):
            comment_sign = "#"
        else:
            return False
        path = Path("tests/queries/0_stateless") / test_source_file
        try:
            with path.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.rstrip("\n")
                    if not line.startswith(comment_sign):
                        continue
                    rest = line[len(comment_sign) :].lstrip()
                    if not rest.startswith("Tags:"):
                        continue
                    tags = {t.strip() for t in rest[len("Tags:") :].split(",")}
                    return "no-parallel" in tags or "sequential" in tags
        except OSError:
            return False
        return False

    @classmethod
    def _is_stateless_harness_file(cls, fpath: str) -> bool:
        return (
            fpath in cls._STATELESS_HARNESS_PATHS
            or fpath.startswith(cls._STATELESS_HARNESS_PATH_PREFIXES)
            or (Path(fpath).parent == Path("tests") and Path(fpath).suffix == ".txt")
        )

    def get_changed_tests(self, strict=False, include_harness_smoke=False):
        # TODO: add support for integration tests
        result = set()
        if hasattr(self, "_diff_text") and self._diff_text:
            # Reuse already-fetched diff text to extract changed file names — avoids
            # a second diff fetch and works when the diff was pre-fetched via --diff-file.
            changed_files = [
                m.group(1)
                for m in re.finditer(r"^\+\+\+ b/(.+)$", self._diff_text, re.MULTILINE)
            ]
        elif self.info.is_local_run:
            changed_files = Shell.get_output(
                f"gh pr diff {self.info.pr_number} --repo ClickHouse/ClickHouse --name-only"
            ).splitlines()
        else:
            changed_files = self.info.get_changed_files()
        if strict and changed_files is None:
            raise RuntimeError(
                "Failed to get changed files required for test selection"
            )
        if not changed_files:
            return result

        if include_harness_smoke and any(
            self._is_stateless_harness_file(fpath) for fpath in changed_files
        ):
            smoke_tests = list(self.STATELESS_HARNESS_SMOKE_TESTS)
            job_name = getattr(self.info, "job_name", "").lower()
            for (
                option,
                feature_smoke_tests,
            ) in self.STATELESS_HARNESS_FEATURE_SMOKE_TESTS.items():
                if option in job_name or job_name == "select functional tests":
                    smoke_tests.extend(feature_smoke_tests)
            print(
                "Functional-test harness changed; adding deterministic smoke tests: "
                f"{smoke_tests}"
            )
            result.update(smoke_tests)

        for fpath in changed_files:
            if not fpath.startswith("tests/queries/0_stateless/"):
                if fpath.startswith("tests/queries/"):
                    # Log any other changed file under tests/queries for future debugging
                    print(
                        f"File '{fpath}' changed, but doesn't match expected test pattern"
                    )
                continue

            if not Path(fpath).exists():
                print(f"File '{fpath}' was removed — skipping")
                continue

            # A file directly at the suite root may itself be a test source, or a
            # supporting file (`.reference`, a `.sql.j2` template, ...) whose sibling
            # test shares its base name.
            if Path(fpath).parent == Path("tests/queries/0_stateless"):
                test_base_name = self._derive_test_name(fpath)
                if test_base_name is not None:
                    print(f"Detected changed test: '{test_base_name}' (from '{fpath}')")
                    # Add '.' suffix to precisely match this test only
                    result.add(f"{test_base_name}.")
                    continue

            # Either a data fixture nested in a subdirectory
            # (`data_parquet/02716_data.parquet`) or a root-level orphan data file
            # (`02995_settings_26_4_1.tsv`) with no sibling test. Map it back to the
            # test(s) that own it so a fixture-only change still reruns the tests
            # that consume it; otherwise the flaky check — and the merge-queue drift
            # guard built on it — would silently skip the very test surface the
            # change affects. Emit only real tests: `_tests_owning_data_file` returns
            # an empty list when nothing maps, so we never fabricate a no-match
            # pattern (which would make clickhouse-test exit 1).
            owning_tests = self._tests_owning_data_file(fpath)
            if owning_tests:
                for base_name in owning_tests:
                    print(
                        f"Detected changed data file '{fpath}' owned by test '{base_name}'"
                    )
                    # Add '.' suffix to precisely match this test only
                    result.add(f"{base_name}.")
            else:
                print(
                    f"File '{fpath}' is not a test source and has no owning test — skipping"
                )

        return sorted(result)

    def get_previously_failed_tests(self):
        assert self.job_type, "Unsupported job type"
        assert (
            self.info.pr_number > 0
        ), "Find tests by previous failures applicable only for PRs"

        tests = []
        cidb = self._ci_db()
        if self.job_type == self.INTEGRATION_JOB_TYPE:
            test_name_pattern = "^test_"
        elif self.job_type == self.STATELESS_JOB_TYPE:
            test_name_pattern = "^[0-9]{5}_"
        else:
            assert False, f"Not supported job type [{self.job_type}]"
        query = FAILED_TESTS_QUERY.format(
            PR_NUMBER=self.info.pr_number,
            JOB_TYPE=self.job_type,
            TEST_NAME_PATTERN=test_name_pattern,
        )
        query_result = cidb.query(query, log_level="") or ""
        # Parse test names from the query result
        for line in query_result.strip().split("\n"):
            if line.strip():
                # Split by whitespace and get the first column (test_name)
                parts = line.split()
                if parts:
                    test_name = parts[0]
                    tests.append(test_name)
        print(f"Parsed {len(tests)} test names: {tests}")
        tests = list(dict.fromkeys(tests))
        # A test that failed within the CIDB window (30 days) may have been
        # deleted or renamed on master since — e.g. a flaky test that was
        # removed instead of deflaked. Passing its name to clickhouse-test (or
        # to the integration runner) selects nothing; with no other targeted
        # tests the run ends with "No tests were run." and exit code 1 (the
        # same failure mode `PR #104097` fixed for orphan data files). Rerun
        # only tests that still exist in this checkout.
        missing = [t for t in tests if not self._test_exists(t)]
        if missing:
            print(
                f"Skipping {len(missing)} previously failed tests that no longer "
                f"exist in the checkout: {missing}"
            )
            tests = [t for t in tests if t not in set(missing)]
        return tests

    def _test_exists(self, test_name: str) -> bool:
        """Whether a test name reported by CIDB still resolves to a test in
        this checkout."""
        if self.job_type == self.INTEGRATION_JOB_TYPE:
            # Integration test names look like
            # `test_storage_kafka/test.py::test_case[param]`; the first path
            # component is the test directory under `tests/integration/`.
            test_dir = test_name.split("/", 1)[0].split("::", 1)[0]
            return (Path("tests/integration") / test_dir).is_dir()
        # Stateless test names are the base name of a file under
        # `tests/queries/0_stateless/` with one of the known extensions.
        test_dir = Path("tests/queries/0_stateless")
        test_name = self.selection_test_name(test_name).rstrip(".")
        return any(
            (test_dir / f"{test_name}{ext}").is_file()
            for ext in self._TEST_FILE_EXTENSIONS
        )

    _stored_path = staticmethod(canonical_coverage_path)

    # Shared-registry files: purely declarative files whose changes are virtually always
    # additive (`extern const Event …`, new setting entries, new error codes).  Every
    # test emits profile events / reads settings, so coverage for any changed line in
    # these files returns thousands of tests and floods the candidate pool with noise
    # (the real signal lives in the other files touched by the same PR).  Skip them
    # from precise coverage admission.
    SHARED_REGISTRY_FILES = frozenset(
        {
            "src/Common/ProfileEvents.cpp",
            "src/Common/ProfileEvents.h",
            "src/Common/CurrentMetrics.cpp",
            "src/Common/CurrentMetrics.h",
            "src/Common/ErrorCodes.cpp",
            "src/Common/ErrorCodes.h",
            "src/Common/SettingsChanges.cpp",
            "src/Core/Settings.cpp",
            "src/Core/SettingsChangesHistory.cpp",
            "src/Core/SettingsChangesHistory.h",
        }
    )

    def coverage_snapshots(self):
        if not hasattr(self, "_coverage_snapshots"):
            cutoff = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            snapshots = parse_rows(
                self._ci_db().query(snapshot_query(cutoff, self.config), log_level="")
            )
            self.selection_diagnostics.update(
                {
                    "cutoff": cutoff,
                    "coverage_snapshots": snapshots,
                    "snapshot_identity": "legacy-shard-timestamp",
                }
            )
            validate_snapshots(snapshots, cutoff, self.config)
            self._coverage_snapshots = snapshots
        return self._coverage_snapshots

    def build_coverage_query(self, changed_lines, hunk_ranges=None):
        return build_candidate_query(
            changed_lines, hunk_ranges or {}, self.coverage_snapshots(), self.config
        )

    def check_coverage_canary(self):
        if self.selection_diagnostics.get("canary", {}).get("status") == "OK":
            return self.selection_diagnostics["canary"]
        snapshots = self.coverage_snapshots()
        # Find an actual narrow region, then use the same diff-path conversion,
        # candidate query and scorer as selection. Never substitute keyword hits.
        query = f"""
            SELECT file, line_start, line_end
            FROM checks_coverage_lines
            WHERE {snapshot_predicate(snapshots)}
              AND (file, line_start, line_end) IN (
                  SELECT file, line_start, line_end FROM checks_coverage_lines
                  WHERE {snapshot_predicate(snapshots)}
                    AND test_name IN ('00001_select_1.sql', '00001_select_1')
                    AND line_end >= line_start
                    AND line_end - line_start + 1 <= {self.config.narrow_region_max_lines}
                    AND (startsWith(file, 'src/') OR startsWith(file, './src/'))
              )
              AND match(test_name, '^[0-9]{{5}}_')
            GROUP BY file, line_start, line_end
            HAVING uniqExact(test_name) <= {self.config.max_precise_region_owners}
            ORDER BY line_end - line_start, file, line_start
            LIMIT 100
            FORMAT JSONEachRow
        """
        started = time.monotonic()
        self.selection_diagnostics["canary"] = {
            "status": "checking",
            "seed_query": query,
        }
        seeds = parse_rows(self._ci_db().query(query, log_level=""))
        if not seeds:
            samples = self._ci_db().query(
                f"SELECT DISTINCT file FROM checks_coverage_lines WHERE {snapshot_predicate(snapshots)} "
                "AND test_name = '00001_select_1.sql' LIMIT 5 FORMAT JSONEachRow",
                log_level="",
            )
            self.selection_diagnostics["canary"]["stored_path_samples"] = parse_rows(
                samples
            )
            raise RuntimeError(
                f"Coverage canary has no usable regions: {self.selection_diagnostics}"
            )
        lines = [
            (canonical_coverage_path(seed["file"]), int(seed["line_start"]))
            for seed in seeds
        ]
        canary = {
            "canonical_input_paths": sorted({path for path, _ in lines}),
            "stored_path_samples": sorted({seed["file"] for seed in seeds}),
            "newest_coverage_timestamp": max(s["check_start_time"] for s in snapshots),
        }
        production_query = self.build_coverage_query(lines)
        canary["query"] = production_query
        self.selection_diagnostics["canary"] = canary
        regions = parse_rows(self._ci_db().query(production_query, log_level=""))
        canary["region_count"] = len(regions)
        candidates = rank_candidates(regions, lines, {}, snapshots, self.config)
        if not any(
            self.selection_test_name(c["test"]) == "00001_select_1." for c in candidates
        ):
            raise RuntimeError(f"Coverage selector canary failed: {json.dumps(canary)}")
        canary.update(
            {
                "status": "OK",
                "region_count": len(regions),
                "query_seconds": time.monotonic() - started,
            }
        )
        return canary

    def get_tests_by_changed_lines(self, changed_lines, hunk_ranges=None):
        coverage_lines = sorted(
            {
                (canonical_coverage_path(path), line)
                for path, line in changed_lines
                if canonical_coverage_path(path).startswith(
                    ("src/", "programs/", "utils/", "base/")
                )
                and canonical_coverage_path(path) not in self.SHARED_REGISTRY_FILES
            }
        )
        self.selection_diagnostics.update(
            {
                "changed_lines": sorted(changed_lines),
                "coverage_lines": coverage_lines,
                "hunk_ranges": hunk_ranges or {},
            }
        )
        if not coverage_lines:
            self._coverage_regions = []
            return []
        self.check_coverage_canary()
        query = self.build_coverage_query(coverage_lines, hunk_ranges)
        started = time.monotonic()
        raw = self._ci_db().query(query, log_level="")
        self._coverage_regions = parse_rows(raw)
        self.selection_diagnostics.update(
            {
                "query": query,
                "query_seconds": time.monotonic() - started,
                "response_bytes": len(raw.encode()),
                "region_count": len(self._coverage_regions),
                "coverage_state": (
                    "precise_matches"
                    if self._coverage_regions
                    else "no_precise_coverage"
                ),
            }
        )
        return rank_candidates(
            self._coverage_regions,
            coverage_lines,
            hunk_ranges or {},
            self.coverage_snapshots(),
            self.config,
        )

    @staticmethod
    def _extract_domain_keywords(filename: str) -> list:
        """
        Extract domain-specific CamelCase words from a source filename.
        These words are diagnostic features for semantic scoring experiments.
        They never admit candidates or replace precise coverage results.

        Returns a list of significant words (length > 4, not architectural).
        An empty list indicates a generic source filename.
        """
        import re as _re
        import os as _os

        base = _os.path.splitext(_os.path.basename(filename))[0]
        # Split CamelCase and all-caps acronyms:
        #   "CHColumnToArrowColumn" → ['CH', 'Column', 'To', 'Arrow', 'Column']
        #   "LDAPClient"            → ['LDAP', 'Client']
        #   "PostgreSQLDictionary"  → ['Postgre', 'SQL', 'Dictionary']
        # Pattern: acronym-run-before-TitleCase | TitleCase-word | lowercase-word
        # The trailing [A-Z] alternative captures single uppercase chars that
        # the other patterns miss (e.g. the "K" in "TopK" or "N" in "MergeN").
        words = _re.findall(
            r"[A-Z]+(?=[A-Z][a-z])|[A-Z][a-z0-9]+|[A-Z]{2,}|[a-z][a-z0-9]+|[A-Z]", base
        )
        # Merge a lone trailing uppercase letter into the previous word so that
        # compound names like "TopK" or "MergeN" are kept whole instead of losing
        # the suffix.
        merged: list = []
        for w in words:
            if len(w) == 1 and w.isupper() and merged:
                merged[-1] = merged[-1] + w
            else:
                merged.append(w)
        words = merged
        # Architectural / ubiquitous words that appear in most files in a directory.
        # Keeping this list generous avoids keywords that are too common to be useful.
        COMMON = {
            # Generic C++ / ClickHouse infrastructure words
            "block",
            "input",
            "output",
            "format",
            "column",
            "stream",
            "storage",
            "table",
            "query",
            "parser",
            "writer",
            "reader",
            "buffer",
            "default",
            "base",
            "impl",
            "merge",
            "tree",
            "row",
            "file",
            "data",
            "info",
            "type",
            "list",
            "map",
            "with",
            "from",
            "into",
            # MergeTree-specific architectural words (appear in almost every MergeTree file)
            "condition",
            "granularity",
            "selector",
            "partition",
            "replica",
            "transaction",
            "virtual",
            "local",
            "remote",
            "range",
            "level",
            # ClickHouse architectural nouns that appear in many places but are not
            # specific enough to pin to a test domain.
            "handler",
            "manager",
            "source",
            "access",
            "control",
            "service",
            "server",
            "client",
            "external",
            "internal",
            "settings",
            "setting",
            "config",
            "context",
            "result",
            "state",
            "status",
            "entry",
            "record",
            "update",
            "create",
        }
        # Allow 3-char all-uppercase acronyms (CSV, ORC, URL, JWT, KQL, etc.) in addition
        # to words ≥ 4 chars.  Generic acronyms like "API", "SQL", "DDL", "DML" are added
        # to COMMON below so they don't generate false matches.
        COMMON_ACRONYMS = {
            "api",
            "sql",
            "ddl",
            "dml",
            "ids",
            "uid",
            "abi",
            "cpu",
            "gpu",
            "ram",
            "tcp",
            "udp",
            "tls",
            "ssl",
            "rpc",
            "ttl",
            "log",
            "tag",
            "row",
            "set",
        }
        specific = [
            w
            for w in words
            if w.lower() not in COMMON
            and (
                (len(w) >= 4)
                or (len(w) == 3 and w.isupper() and w.lower() not in COMMON_ACRONYMS)
            )
        ]
        return specific

    def get_changed_or_new_tests_with_info(
        self, strict=False, include_harness_smoke=False
    ):
        tests = sorted(
            self.get_changed_tests(
                strict=strict, include_harness_smoke=include_harness_smoke
            )
        )
        info = f"Found {len(tests)} changed or new tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that were changed or added",
            status=Result.Status.OK,
            info=info,
        )

    def get_previously_failed_tests_with_info(self, strict=False):
        try:
            tests = self.get_previously_failed_tests()
        except Exception as e:
            if strict:
                raise RuntimeError(
                    "Failed to get previously failed tests required for test selection"
                ) from e
            print(
                f"WARNING: Failed to get previously failed tests (best effort): {e}",
                file=sys.stderr,
            )
            tests = []
        # TODO: add job name to the result.info
        info = f"Found {len(tests)} previously failed tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that failed in previous runs",
            status=Result.Status.OK,
            info=info,
        )

    @staticmethod
    def _parse_diff_lines(diff_text: str) -> list:
        """
        Parse a unified diff and return `(filename, line_no)` tuples for every changed line.
        Line numbers are old-file positions (from the `-a,b` hunk header), which match the
        master-build coverage data stored in CIDB.  For `-` lines the old-file line number
        is used directly; for `+` lines (pure additions) the current old-file position
        (insertion point) is used so that the surrounding function is still found in CIDB.
        """
        changed: set = set()
        current_file = None
        old_line = 0
        in_hunk = False
        for line in diff_text.splitlines():
            if line.startswith("diff --git "):
                current_file = None
                in_hunk = False
            elif line.startswith("--- "):
                current_file = line[6:] if line.startswith("--- a/") else None
                in_hunk = False
            elif line.startswith("+++ b/"):
                if current_file is None:
                    current_file = line[6:]
                in_hunk = False
            elif line.startswith("@@ ") and current_file:
                m = re.search(r"-(\d+)", line)
                old_line = int(m.group(1)) if m else 0
                in_hunk = True
            elif in_hunk:
                if line.startswith("-"):
                    changed.add((current_file, old_line))
                    old_line += 1
                elif line.startswith("+"):
                    changed.add((current_file, old_line))  # insertion point in old file
                elif line.startswith(" "):
                    old_line += 1  # context line
        return sorted(changed)

    @staticmethod
    def _parse_diff_hunk_ranges(diff_text: str) -> dict:
        """
        Parse a unified diff and return per-file hunk boundary ranges.

        Returns `{filename: [(hunk_start, hunk_end), ...]}` where each tuple is the
        old-file start and end line of a hunk.  These ranges are used to expand the
        CIDB range query beyond just the actually-changed lines so that coverage
        regions at unchanged context lines adjacent to the change are also captured.

        Example: a hunk `@@ -331,6 ... @@` contains 6 old-file lines (331-336).
        If CIDB only records a coverage point at line 332 (the first statement
        after a branch), a query using only the changed lines {331} would miss it,
        but including the full hunk range [331, 336] finds it.
        """
        hunks: dict = {}  # filename -> list of (start, end)
        current_file = None
        for line in diff_text.splitlines():
            if line.startswith("diff --git "):
                current_file = None
            elif line.startswith("--- "):
                current_file = line[6:] if line.startswith("--- a/") else None
            elif line.startswith("+++ b/"):
                if current_file is None:
                    current_file = line[6:]
            elif line.startswith("@@ ") and current_file:
                # @@ -old_start,old_count +new_start,new_count @@
                m = re.search(r"-(\d+)(?:,(\d+))?", line)
                if m:
                    start = int(m.group(1))
                    count = int(m.group(2)) if m.group(2) is not None else 1
                    end = max(start, start + count - 1)
                    hunks.setdefault(current_file, []).append((start, end))
        return hunks

    def get_diff_text(self) -> str:
        """Fetch the PR diff text (cached on self._diff_text after first call).

        CI containers have no `.git` directory. Fetch a comparison pinned to the PR SHA.
        For public repos no auth is needed; for private repos GITHUB_TOKEN is used.
        """
        if hasattr(self, "_diff_text") and self._diff_text is not None:
            return self._diff_text
        assert self.info.pr_number > 0, "Diff fetching applicable for PRs only"
        repo = self.info.repo_name or "ClickHouse/ClickHouse"
        if self.info.is_local_run:
            self._diff_text = Shell.get_output(
                f"gh pr diff {self.info.pr_number} --repo {repo}"
            )
        else:
            import requests

            headers = {"Accept": "application/vnd.github+json"}
            token = os.environ.get("GITHUB_TOKEN", "")
            if token:
                headers["Authorization"] = f"Bearer {token}"
            response = requests.get(
                f"https://api.github.com/repos/{repo}/pulls/{self.info.pr_number}",
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            metadata = response.json()
            if metadata["head"]["sha"] != self.info.sha:
                raise RuntimeError(
                    "PR head changed before test selection; refusing a diff for a different SHA"
                )
            base_sha = metadata["base"]["sha"]
            headers["Accept"] = "application/vnd.github.diff"
            response = requests.get(
                f"https://api.github.com/repos/{repo}/compare/{base_sha}...{self.info.sha}",
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            self._diff_text = response.text
            self.selection_diagnostics["diff_base_sha"] = base_sha
        return self._diff_text

    def get_changed_lines_from_diff(self):
        """
        Return changed lines from the PR diff.
        In CI fetches the diff from GitHub API; for local runs uses `gh pr diff`.
        """
        assert self.info.pr_number > 0, "Find tests by diff applicable for PRs only"
        return self._parse_diff_lines(self.get_diff_text())

    @classmethod
    def selection_test_name(cls, test):
        name = cls.normalize_stateless_test_name(test).rstrip(".")
        for extension in cls._TEST_FILE_EXTENSIONS:
            if name.endswith(extension):
                name = name[: -len(extension)]
                break
        return name + "."

    def get_most_relevant_tests(self):
        changed_lines = self.get_changed_lines_from_diff()
        hunk_ranges = self._parse_diff_hunk_ranges(self.get_diff_text())
        candidates = self.get_tests_by_changed_lines(changed_lines, hunk_ranges)
        self._coverage_candidates = []
        missing = []
        keywords = {
            word.lower()
            for path in {path for path, _ in changed_lines}
            for word in self._extract_domain_keywords(path)
        }
        for candidate in candidates:
            candidate["semantic_keyword_matches"] = sorted(
                word for word in keywords if word in candidate["test"].lower()
            )
            if self.functional_test_source_file(
                self.selection_test_name(candidate["test"])
            ):
                self._coverage_candidates.append(candidate)
            else:
                missing.append(
                    {**candidate, "admission_reason": "test_missing_in_checkout"}
                )
        self.selection_diagnostics["missing_tests"] = missing
        # All experiments consume the same response; entry count has no effect
        # on the executed list until pre-PR replay validates its direction.
        if changed_lines and getattr(self, "_coverage_regions", []):
            self.selection_diagnostics["shadow"] = {
                mode: rank_candidates(
                    self._coverage_regions,
                    changed_lines,
                    hunk_ranges,
                    self.coverage_snapshots(),
                    self.config,
                    entry_mode=mode,
                )
                for mode in ("legacy-tier", "relative-low", "relative-high")
            }
        tests = [c["test"] for c in self._coverage_candidates]
        return tests, Result(
            name="tests found by coverage",
            status=Result.Status.OK,
            info=f"Found {len(tests)} precise coverage candidates; entry-count scoring disabled",
        )

    def get_all_relevant_tests_with_info(self, include_changed_tests=True):
        if self.job_type == self.STATELESS_JOB_TYPE:
            self.get_diff_text()
        results = []
        changed = []
        if include_changed_tests and self.job_type == self.STATELESS_JOB_TYPE:
            changed, result = self.get_changed_or_new_tests_with_info(
                strict=True, include_harness_smoke=True
            )
            results.append(result)
        failed, result = self.get_previously_failed_tests_with_info(strict=True)
        results.append(result)
        candidates = []
        if self.job_type == self.STATELESS_JOB_TYPE:
            _, result = self.get_most_relevant_tests()
            candidates = self._coverage_candidates
            results.append(result)
        normalize = (
            self.selection_test_name
            if self.job_type == self.STATELESS_JOB_TYPE
            else lambda test: test
        )
        selection = protect_selection(
            changed, failed, candidates, normalize, self.config
        )
        self.selection_diagnostics.update(
            {
                "selector_version": self.config.version,
                "coverage_path_version": self.config.path_version,
                "config": asdict(self.config),
                "candidates": candidates,
                **selection,
            }
        )
        ranked = [record["test"] for record in selection["selected"]]
        details = f"Found {len(ranked)} relevant tests; {len(selection['rejected'])} excluded by temporary ceiling"
        if selection["mandatory_overflow"]:
            details += f"; mandatory tests exceed ceiling by {selection['mandatory_overflow']} (all retained)"
        print(details)
        return ranked, Result(
            name="Fetch relevant tests",
            status=Result.Status.OK,
            info=details,
            results=results,
        )


if __name__ == "__main__":
    # local run: use the same pipeline as CI (get_all_relevant_tests_with_info)
    parser = argparse.ArgumentParser(
        description="List tests covering changed lines for a PR by querying the coverage database."
    )
    parser.add_argument("pr", help="Pull request number")
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Run only the coverage-based pass (get_most_relevant_tests), skip changed-file "
        "and previously-failed passes. Uses one fewer GitHub API call — useful for eval.",
    )
    parser.add_argument(
        "--diff-file",
        default=None,
        help="Path to a pre-fetched unified diff file. When provided, "
        "get_changed_lines_from_diff reads from this file instead of calling gh.",
    )
    args = parser.parse_args()

    class InfoLocalTest:
        pr_number = int(args.pr)
        is_local_run = True
        job_name = "Stateless"

    info = InfoLocalTest()
    targeting = Targeting(info)

    # If a pre-fetched diff file is provided, monkey-patch get_diff_text so both
    # get_changed_lines_from_diff and get_most_relevant_tests read from the file
    # rather than fetching the diff.
    if args.diff_file:
        diff_text = Path(args.diff_file).read_text()
        targeting._diff_text = diff_text

    if args.coverage_only:
        ranked, result = targeting.get_most_relevant_tests()
    else:
        ranked, result = targeting.get_all_relevant_tests_with_info()

    print(f"\nAll selected tests ({len(ranked)}):")
    for test in ranked:
        print(f" {test}")
    print(f"\nFound {len(ranked)} relevant tests")
