import argparse
import ast
import os
import re
import sys
from pathlib import Path

sys.path.append("./")

from ci.praktika.cidb import CIDB
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

# Coverage data lives in the public ClickHouse CIDB, accessible from any CI environment.
# Use this URL for all coverage queries so that private-repo CI (which may not have
# access to an internal CIDB) can still query test coverage data.
_PUBLIC_CIDB_URL = "https://play.clickhouse.com"

# Query to fetch failed tests from CIDB for a given PR.
# Pre-filters out commit/check_name combinations with >= 20 failures — these indicate
# widespread failures (e.g. build broken, environment issue) where every test failed,
# not genuine per-test flakiness.  Results are ordered by recency-weighted failure
# count (exponential decay, 7-day half-life). Capped at 100.
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
 limit 100 \
"""


class Targeting:
    INTEGRATION_JOB_TYPE = "Integration"
    STATELESS_JOB_TYPE = "Stateless"

    def __init__(self, info: Info):
        self.info = info
        if "stateless" in info.job_name.lower():
            self.job_type = self.STATELESS_JOB_TYPE
        elif "integration" in info.job_name.lower():
            self.job_type = self.INTEGRATION_JOB_TYPE
        else:
            self.job_type = None

    def get_changed_tests(self):
        # TODO: add support for integration tests
        result = set()
        if hasattr(self, '_diff_text') and self._diff_text:
            # Reuse already-fetched diff text to extract changed file names — avoids
            # a second diff fetch and works when the diff was pre-fetched via --diff-file.
            changed_files = [
                m.group(1)
                for m in re.finditer(r'^\+\+\+ b/(.+)$', self._diff_text, re.MULTILINE)
            ]
        elif self.info.is_local_run:
            changed_files = Shell.get_output(
                f"gh pr diff {self.info.pr_number} --repo ClickHouse/ClickHouse --name-only"
            ).splitlines()
        else:
            changed_files = self.info.get_changed_files()
        if not changed_files:
            return result

        for fpath in changed_files:
            if re.match(r"tests/queries/0_stateless/\d{5}", fpath):
                if not Path(fpath).exists():
                    print(f"File '{fpath}' was removed — skipping")
                    continue

                print(f"Detected changed test file: '{fpath}'")

                fname = os.path.basename(fpath)
                fname_without_ext = os.path.splitext(fname)[0]

                # Add '.' suffix to precisely match this test only
                result.add(f"{fname_without_ext}.")

            elif fpath.startswith("tests/queries/"):
                # Log any other changed file under tests/queries for future debugging
                print(
                    f"File '{fpath}' changed, but doesn't match expected test pattern"
                )

        return sorted(result)

    def get_previously_failed_tests(self):
        from ci.praktika.cidb import CIDB
        from ci.praktika.settings import Settings

        assert self.job_type, "Unsupported job type"
        assert (
            self.info.pr_number > 0
        ), "Find tests by previous failures applicable only for PRs"

        tests = []
        cidb = CIDB(url=Settings.CI_DB_READ_URL, user="play", passwd="")
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
        tests = list(set(tests))
        return sorted(tests)

    @staticmethod
    def _escape_sql_string(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")

    @staticmethod
    def _stored_path(path: str) -> str:
        """Convert a repo-relative diff path to the stored coverage path format.

        Coverage data is built with -ffile-prefix-map=/ClickHouse=. so all
        source paths are stored as ./src/... in checks_coverage_lines.
        Strip any leading ./ from the diff path then re-add the ./ prefix.
        """
        p = path.replace("\\", "/").lstrip("./")
        return "./" + p

    # Regions wider than this are considered "broad" (low signal).
    NARROW_REGION_MAX_LINES = 40

    # Regions covered by more tests than this are skipped from Pass 1 (direct line
    # coverage).  Infrastructure files like SignalHandlers.cpp, Context.cpp, and
    # Settings.cpp are touched by almost every test — a changed line in such a file
    # has no diagnostic value for test selection.  Skipping regions with too many
    # owners prevents these files from flooding primary_tests (which would then
    # cause sibling/indirect queries to fail with HTTP form-field-too-long errors
    # and inflate unique_tests to nearly the full suite).
    MAX_TESTS_PER_LINE = 150

    # Synthetic width for tests found via indirect-call (virtual dispatch) co-occurrence.
    # Lower than SIBLING_DIR_WIDTH (10000) because callee co-occurrence is a stronger
    # signal than directory proximity: tests that call the same vtable slots as primary
    # tests are exercising the same interface, not merely a related file.
    INDIRECT_CALL_WIDTH = 2000

    # Synthetic width assigned to tests found via same-directory sibling file
    # expansion (secondary pass).  Must be >> NARROW_REGION_MAX_LINES so they
    # never get the narrow-tier bonus and always rank below direct hits.
    SIBLING_DIR_WIDTH = 3000

    # Per-pass score multipliers applied on top of the 1/(width×rc) signal.
    # Pass 1 (direct line coverage) is the baseline.  Passes 2 and 3 use
    # secondary signals and are discounted so that even a weak direct hit
    # outranks the strongest indirect or sibling hit.
    PASS_WEIGHT_DIRECT        = 1.0   # Pass 1: test directly covers changed lines
    PASS_WEIGHT_HUNK_CONTEXT  = 0.50  # Pass 1b: test covers context line within the diff hunk
    PASS_WEIGHT_INDIRECT      = 0.50  # Pass 3: test shares virtual-dispatch callees with primary tests
    PASS_WEIGHT_BROAD2        = 0.40  # Pass 4: test covers changed files via very-broad regions (rc 2001-8000)
    PASS_WEIGHT_SIBLING       = 0.25  # Pass 2: test covers a sibling file in the same source directory
    PASS_WEIGHT_KEYWORD       = 0.20  # Fallback: test filename contains domain keywords from changed files

    def get_tests_by_changed_lines(self, changed_lines: list,
                                   hunk_ranges: dict | None = None) -> dict:
        """
        Query `checks_coverage_lines` for tests that cover each (filename, line_no) pair.

        `changed_lines` is a list of `(filename, line_no)` tuples.
        `hunk_ranges` is an optional `{filename: [(start, end), ...]}` dict from
        `_parse_diff_hunk_ranges`.  When provided, the SQL pre-filter for `.cpp`
        files uses the union of all hunk ranges instead of just `[min, max]` of the
        changed lines.  This ensures coverage points at unchanged context lines
        adjacent to the actual change are also returned by the query (they are then
        matched against truly-changed `line_no` values in the Python post-filter).
        Returns a dict mapping each input tuple to a list of `(test_name, region_width)`
        tuples, where `region_width = line_end - line_start + 1`.  The region width is
        used by the caller to weight test scores (narrow regions = high signal).

        All matching regions are included; the scoring formula naturally penalises
        regions covered by many tests via the `region_test_count` denominator.
        """
        import time
        t0 = time.monotonic()
        assert self.job_type, "get_tests_by_changed_lines requires a known job type"

        if not changed_lines:
            return {fl: [] for fl in changed_lines}

        # Filter to files that are actually tracked in checks_coverage_lines.
        # Coverage is only collected for compiled C++ sources (src/, programs/,
        # utils/ etc.).  Querying test scripts, docs, CI configs, or contrib files
        # always returns zero rows and wastes CIDB quota — skip them up front.
        COVERAGE_TRACKED_PREFIXES = ("src/", "programs/", "utils/", "base/")
        coverage_lines = [
            (f, ln)
            for f, ln in changed_lines
            if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
        ]
        skipped = len(changed_lines) - len(coverage_lines)
        if skipped:
            print(
                f"[find_tests] skipping {skipped} lines in non-tracked files "
                f"(test scripts, docs, CI, contrib)"
            )

        # Return the original (full) key-set in the result dict so that
        # callers always get one entry per input line (with empty list for
        # non-tracked files).
        non_tracked_keys = {(f, ln) for f, ln in changed_lines if (f, ln) not in set(coverage_lines)}
        base_result: dict = {k: [] for k in non_tracked_keys}

        if not coverage_lines:
            print("[find_tests] no coverage-tracked files changed — skipping CIDB query")
            base_result.update({(f, ln): [] for f, ln in changed_lines})
            return base_result

        # Group changed lines by file so we query with `file IN (…)` instead of
        # one condition per line.  A PR touching 13 files but 1000+ lines would
        # otherwise generate a URL too long for the CIDB HTTP endpoint.
        files_to_lines: dict = {}
        for f, ln in coverage_lines:
            files_to_lines.setdefault(self._stored_path(f), set()).add(ln)

        unique_files = sorted(files_to_lines)
        print(
            f"[find_tests] querying coverage for {len(coverage_lines)} changed lines "
            f"across {len(unique_files)} files"
        )

        # Build one condition per file: file='X' AND <range condition>.
        # This pre-filters to regions that *could* overlap any changed line in that file,
        # avoiding a full table scan per file while keeping the query size O(files) not O(lines).
        #
        # For header files (.h), we expand the line range to the entire file because
        # headers typically define a single class and a change to any method affects
        # the class semantics.  Tests covering OTHER methods in the same header are
        # highly relevant.  This mirrors the old symbol-level algo which matched all
        # symbols in the same translation unit.
        #
        # For .cpp files, we use per-hunk range conditions when hunk_ranges is provided.
        # This expands the SQL filter to cover context lines within each hunk so that
        # coverage points at unchanged lines adjacent to the actual change are fetched.
        # Example: a one-line change in @@ -331,6 @@ has context lines 332-336; CIDB may
        # only record coverage at line 332 (the first statement in the branch).  Without
        # hunk expansion the query would use line_end >= 331 AND line_start <= 331 and
        # miss this point; with hunk expansion it uses line_end >= 331 AND line_start <= 336
        # and finds it.  The Python post-filter still only assigns results to actually-changed
        # lines, so no spurious tests are introduced — only the SQL range is widened.
        per_file_conds_parts = []
        for f, lines in sorted(files_to_lines.items()):
            esc_f = self._escape_sql_string(f)
            if hunk_ranges:
                # Use the bounding box of all hunk ranges (min hunk start to max hunk end)
                # as the SQL pre-filter for .cpp files.  This covers:
                # 1. Context lines at the start/end of each hunk (same as per-hunk OR).
                # 2. Coverage points that fall BETWEEN two hunks in the same file —
                #    a common case when a PR changes two separate sections of a function.
                #    Example: S3RequestSettings.cpp with hunks (301,306) and (321,326)
                #    has coverage at line 312 (between the hunks, rc=107) that would be
                #    missed by per-hunk OR but is captured by the bounding box [301, 326].
                # Derive the original (non-stored) filename from the stored path for lookup.
                orig_f = f[2:] if f.startswith("./") else f  # strip "./" prefix
                file_hunks = hunk_ranges.get(orig_f, [])
                if file_hunks:
                    # Merge hunks that are ≤ MERGE_GAP lines apart so coverage
                    # points between two nearby hunks (inter-hunk lines) are
                    # included.  Still uses per-range conditions rather than a
                    # single bounding box to avoid vacuuming thousands of lines
                    # between distant hunks in the same file.
                    MERGE_GAP = 5
                    merged: list = [list(file_hunks[0])]
                    for h_start, h_end in file_hunks[1:]:
                        if h_start - merged[-1][1] <= MERGE_GAP:
                            merged[-1][1] = max(merged[-1][1], h_end)
                        else:
                            merged.append([h_start, h_end])
                    hunk_conds = " OR ".join(
                        f"(line_end >= {max(0, h_start - 1)} AND line_start <= {h_end + 1})"
                        for h_start, h_end in merged
                    )
                    per_file_conds_parts.append(f"(file = '{esc_f}' AND ({hunk_conds}))")
                else:
                    # No hunk info for this file; fall back to min/max of changed lines.
                    per_file_conds_parts.append(
                        f"(file = '{esc_f}'"
                        f" AND line_end >= {min(lines)} AND line_start <= {max(lines)})"
                    )
            else:
                per_file_conds_parts.append(
                    f"(file = '{self._escape_sql_string(f)}'"
                    f" AND line_end >= {min(lines)} AND line_start <= {max(lines)})"
                )
        per_file_conds = " OR ".join(per_file_conds_parts)

        # Two-tier query: primary (narrow, <= MAX_TESTS_PER_LINE tests per
        # region) and broad (> MAX, <= BROAD_REGION_HARD_CAP).  Broad regions
        # are included so infrastructure files (Context.cpp, RemoteQueryExecutor,
        # etc.) are not completely invisible — the scoring formula already
        # penalises them via the 1/region_test_count denominator.  Only truly
        # ubiquitous regions (> HARD_CAP) are dropped to keep response size sane.
        BROAD_REGION_HARD_CAP = 3000  # scoring handles broad regions; very ubiquitous dropped
        query = f"""
        SELECT
            file,
            line_start,
            line_end,
            groupArray(test_name) AS tests,
            groupArray(min_depth) AS depths,
            uniqExact(test_name) AS region_test_count
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND ({per_file_conds})
        GROUP BY file, line_start, line_end
        HAVING region_test_count <= {BROAD_REGION_HARD_CAP}
        """

        cidb = CIDB(url=_PUBLIC_CIDB_URL, user="play", passwd="")
        t_query = time.monotonic()
        raw = cidb.query(query, log_level="") or ""
        print(f"[find_tests] CIDB query: {time.monotonic()-t_query:.2f}s, response={len(raw)} bytes")

        # Parse TSV: file \t line_start \t line_end \t [tests] \t [depths] \t region_test_count
        # region_test_count: how many distinct tests cover this region (ownership denominator).
        # Falls back gracefully when columns are absent (old CIDB schema).
        coverage_ranges: list = []
        for row in raw.strip().splitlines():
            if not row:
                continue
            parts = row.split("\t", 5)
            if len(parts) < 4:
                continue
            file_, line_start_s, line_end_s, tests_raw = parts[:4]
            depths_raw = parts[4] if len(parts) >= 5 else None
            count_raw  = parts[5] if len(parts) >= 6 else None
            try:
                line_start = int(line_start_s)
                line_end = int(line_end_s)
                tests = ast.literal_eval(tests_raw.strip())
                depths = ast.literal_eval(depths_raw.strip()) if depths_raw else None
                region_test_count = int(count_raw.strip()) if count_raw else len(tests) if isinstance(tests, list) else 1
                if not isinstance(tests, list):
                    continue
                # Pair each test with its min_depth (255 = not available).
                if isinstance(depths, list) and len(depths) == len(tests):
                    test_depths = [(t, int(d)) for t, d in zip(tests, depths)]
                else:
                    test_depths = [(t, 255) for t in tests]
                coverage_ranges.append((file_, line_start, line_end, test_depths, region_test_count))
            except (ValueError, SyntaxError):
                print(f"Failed to parse coverage row: {row[:100]}")

        # --- Second tier: very broad regions (rc > BROAD_REGION_HARD_CAP) --------
        # Infrastructure files (Context.cpp, ProcessList.cpp) have regions covered
        # by 3000-8000+ tests.  These are dropped by the primary query to keep its
        # response size manageable (groupArray would return thousands of names per
        # region).  However, the tests covering these broad regions are still
        # relevant — they just have low specificity.
        #
        # The second-tier query returns only DISTINCT test names (no per-region
        # grouping) for regions above the primary cap.  This keeps the response
        # size linear in the number of unique tests rather than quadratic in
        # (regions × tests_per_region).  Tests found this way are assigned the
        # BROAD_FALLBACK_WIDTH and a synthetic region_test_count equal to the
        # average rc across all matching broad regions.
        # Skip the broad-tier2 query if we've already consumed too much time.
        elapsed_after_primary = time.monotonic() - t0
        run_broad_tier2 = elapsed_after_primary < 8.0
        if not run_broad_tier2:
            print(f"[find_tests] skipping broad-tier2 query (elapsed={elapsed_after_primary:.1f}s)")

        VERY_BROAD_REGION_CAP = 8000  # drop truly ubiquitous regions
        # Query broad regions and count how many regions each test covers in the
        # changed files.  Tests covering more regions get proportionally higher
        # scores, so tests that genuinely exercise the changed code path (even via
        # broad Context.cpp/ProcessList.cpp regions) rank above tests that merely
        # touch one broad region.  ORDER BY cov_regions DESC + LIMIT ensures we
        # prioritise the most-covering tests when truncating.
        # Rank broad-tier2 tests by (cov_regions * files_covered) so that tests
        # exercising MULTIPLE changed files via broad regions rank above tests that
        # only cover a single changed file.  This makes the guarantee selection more
        # specific: a test covering 3 changed files at 74 broad regions each ranks
        # above a test covering only 1 file at 74 regions.
        broad_query = f"""
        SELECT test_name, count() AS cov_regions, uniqExact(file) AS files_covered
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND ({per_file_conds})
          AND (file, line_start, line_end) IN (
              SELECT file, line_start, line_end
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                AND ({per_file_conds})
              GROUP BY file, line_start, line_end
              HAVING uniqExact(test_name) > {BROAD_REGION_HARD_CAP}
                 AND uniqExact(test_name) <= {VERY_BROAD_REGION_CAP}
          )
        GROUP BY test_name
        ORDER BY (cov_regions * uniqExact(file)) DESC
        LIMIT 8000
        """
        # Broad-tier2 tests have real coverage but from regions shared by
        # thousands of tests.  The effective score is:
        #   cov_regions × PASS_WEIGHT_BROAD_TIER2 / (BROAD_FALLBACK_WIDTH × BROAD_TIER2_RC)
        # = cov_regions × 2e-7
        # A test covering 100 broad regions scores 2e-5 (much higher than the
        # flat 2e-7 the old single-injection approach gave every test).
        BROAD_FALLBACK_WIDTH = 100  # synthetic width — broad but real coverage
        # Synthetic region_test_count: set so that PASS_WEIGHT_BROAD2 / (BROAD_FALLBACK_WIDTH
        # × BROAD_TIER2_RC) stays in the same absolute score range as before while correctly
        # reflecting that broad-tier2 signal is weaker than indirect-call (0.15 < 0.30).
        # With PASS_WEIGHT_BROAD2=0.15 and the same score target (1e-5 at cov_regions=50,
        # effective_width=10): BROAD_TIER2_RC = 0.15 / (10 × 1e-5) = 1500.
        BROAD_TIER2_RC = 600
        PASS_WEIGHT_BROAD_TIER2 = self.PASS_WEIGHT_BROAD2

        broad_tests: dict = {}   # test_name -> cov_regions count
        if run_broad_tier2:
            t_broad = time.monotonic()
            try:
                broad_raw = cidb.query(broad_query, log_level="") or ""
                broad_elapsed = time.monotonic() - t_broad
                print(f"[find_tests] broad-tier2 query: {broad_elapsed:.2f}s, response={len(broad_raw)} bytes")
            except Exception as e:
                print(f"[find_tests] broad-tier2 query failed (non-fatal): {e}")
                broad_raw = ""

            # Parse broad-tier2 results: test_name \t cov_regions \t files_covered
            # Store cov_regions * files_covered as the ranking key so tests covering
            # multiple changed files via broad regions rank above single-file tests.
            for row in broad_raw.strip().splitlines():
                parts = row.strip().split("\t", 2)
                if not parts[0]:
                    continue
                cov_regions = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
                files_covered = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
                broad_tests[parts[0]] = cov_regions * files_covered
            if broad_tests:
                max_score = max(broad_tests.values()) if broad_tests else 0
                print(
                    f"[find_tests] broad-tier2: {len(broad_tests)} additional tests from "
                    f"regions with {BROAD_REGION_HARD_CAP} < rc <= {VERY_BROAD_REGION_CAP} "
                    f"(top score={max_score})"
                )

        # Map each input (filename, line_no) to (test_name, region_width, min_depth,
        # region_test_count) 4-tuples.  The CIDB query returned all ranges for the
        # touched files; now filter to ranges that actually overlap a changed line.
        # Start from base_result which already has empty entries for non-tracked files.
        # Pre-index coverage_ranges by file for faster lookup.
        ranges_by_file: dict = {}
        for entry in coverage_ranges:
            ranges_by_file.setdefault(entry[0], []).append(entry)

        # Build a per-file, per-changed-line hunk-range lookup: for each changed line,
        # find the hunk (start, end) that contains it.  A coverage region is relevant
        # for a changed line if it overlaps either the line itself OR the hunk containing
        # the line.  This ensures regions at unchanged context lines (e.g. the `if`
        # condition at the start of a hunk) are counted for the changed lines inside it.
        # The region width used for scoring is the actual CIDB region width (not the hunk
        # width) so the scoring formula remains correct.
        line_to_hunk: dict = {}  # (filename, line_no) -> (hunk_start, hunk_end) or None
        if hunk_ranges:
            for filename, line_no in coverage_lines:
                orig_f = filename  # coverage_lines use original paths
                file_hunks = hunk_ranges.get(orig_f, [])
                for h_start, h_end in file_hunks:
                    if h_start <= line_no <= h_end:
                        line_to_hunk[(filename, line_no)] = (h_start, h_end)
                        break

        result: dict = dict(base_result)
        for filename, line_no in coverage_lines:
            stored = self._stored_path(filename)
            matched: list = []
            is_header = filename.endswith(".h")
            hunk = line_to_hunk.get((filename, line_no))  # (hunk_start, hunk_end) or None
            for file_, line_start, line_end, test_depths, region_test_count in ranges_by_file.get(stored, []):
                # Direct overlap: region contains the changed line.
                overlaps = line_start <= line_no <= line_end
                # Hunk overlap: region overlaps the hunk containing the changed line.
                # Used for regions at context lines adjacent to the actual change.
                hunk_overlap = (
                    not overlaps and hunk is not None and
                    line_start <= hunk[1] and line_end >= hunk[0]
                )
                if overlaps:
                    width = line_end - line_start + 1
                    for t, depth in test_depths:
                        matched.append((t, width, depth, region_test_count,
                                        self.PASS_WEIGHT_DIRECT))
                elif hunk_overlap:
                    # Context-line hit: region is within the diff hunk but does not
                    # directly contain the changed line.  Use PASS_WEIGHT_HUNK_CONTEXT
                    # (0.35 < PASS_WEIGHT_DIRECT but > PASS_WEIGHT_INDIRECT) so:
                    # - Scores rank below direct hits but above indirect/sibling.
                    # - The pass_weight is below PASS_WEIGHT_INDIRECT (0.3)... wait
                    #   actually 0.35 > 0.3; sparse-file threshold uses pw >= PASS_WEIGHT_INDIRECT,
                    #   so hunk-context hits WOULD still count.  But we explicitly want them
                    #   NOT to count — the file has nearby coverage but the changed lines
                    #   themselves are in a code path with low rc (pure insertions).
                    #   Using PASS_WEIGHT_HUNK_CONTEXT in the sparse-file check is handled
                    #   by the condition: sparse-file uses pw >= PASS_WEIGHT_DIRECT (see below).
                    width = line_end - line_start + 1
                    for t, depth in test_depths:
                        matched.append((t, width, depth, region_test_count,
                                        self.PASS_WEIGHT_HUNK_CONTEXT))
                elif is_header:
                    # For header files: include non-overlapping regions from
                    # the same file with SIBLING_DIR_WIDTH penalty.  These are
                    # other methods in the same class, still relevant but with
                    # weaker signal than a direct line overlap.
                    for t, depth in test_depths:
                        matched.append((t, self.SIBLING_DIR_WIDTH, 255, region_test_count,
                                        self.PASS_WEIGHT_DIRECT))
            # Deduplicate: keep best (min width, max pass_weight, min depth, min rc) per test.
            by_test: dict = {}  # test -> (min_width, min_depth, min_region_test_count, max_pw)
            for t, w, d, rc, pw in matched:
                if t not in by_test:
                    by_test[t] = (w, d, rc, pw)
                else:
                    ow, od, orc, opw = by_test[t]
                    by_test[t] = (min(ow, w), min(od, d), min(orc, rc), max(opw, pw))
            result[(filename, line_no)] = [
                (t, w, d, rc, pw) for t, (w, d, rc, pw) in sorted(by_test.items())
            ]

        # Inject broad-tier2 tests into coverage results.
        # Tests are injected into the first coverage-tracked changed line only.
        # The effective width is inversely proportional to cov_regions so that
        # tests covering more of the changed files get lower width → higher score:
        #   score = PASS_WEIGHT_BROAD_TIER2 / (effective_width × BROAD_TIER2_RC)
        #         = 0.5 / (max(1, 500//cov_regions) × 5000)
        # A test covering 100 regions: effective_width=5, score=0.5/(5×5000)=2e-5
        # A test covering   1 region:  effective_width=500, score=0.5/(500×5000)=2e-7
        # The 100x difference correctly reflects relative specificity.
        if broad_tests:
            for filename, line_no in coverage_lines:
                if not any(filename.startswith(p) for p in COVERAGE_TRACKED_PREFIXES):
                    continue
                key = (filename, line_no)
                existing = {e[0] for e in result.get(key, [])}
                for tname, cov_regions in broad_tests.items():
                    if tname not in existing:
                        effective_width = max(1, BROAD_FALLBACK_WIDTH // max(1, cov_regions))
                        result.setdefault(key, []).append(
                            (tname, effective_width, 255, BROAD_TIER2_RC, PASS_WEIGHT_BROAD_TIER2)
                        )
                break  # inject into first tracked line only (score is cov_regions-weighted)

        # --- Third tier: ultra-broad expansion -----------------------------------
        # When both primary (rc ≤ BROAD_REGION_HARD_CAP) and broad-tier2
        # (BROAD_REGION_HARD_CAP < rc ≤ VERY_BROAD_REGION_CAP) return no tests for
        # all changed lines, expand further to rc ≤ ULTRA_BROAD_REGION_CAP.
        # This handles infrastructure files where every changed line is covered by
        # 8000+ tests (e.g. IMergeTreeDataPart.cpp, Context.cpp hot paths) so
        # neither tier finds anything.  Ultra-broad tests rank below all other
        # evidence — they are a last resort before the keyword fallback.
        all_found = {e[0] for pairs in result.values() for e in pairs}
        if not all_found and run_broad_tier2 and (time.monotonic() - t0) < 12.0:
            ULTRA_BROAD_REGION_CAP = 30000
            ultra_query = f"""
            SELECT test_name, count() AS cov_regions, uniqExact(file) AS files_covered
            FROM checks_coverage_lines
            WHERE check_start_time > now() - interval 3 days
              AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              AND notEmpty(test_name)
              AND ({per_file_conds})
              AND (file, line_start, line_end) IN (
                  SELECT file, line_start, line_end
                  FROM checks_coverage_lines
                  WHERE check_start_time > now() - interval 3 days
                    AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                    AND ({per_file_conds})
                  GROUP BY file, line_start, line_end
                  HAVING uniqExact(test_name) > {VERY_BROAD_REGION_CAP}
                     AND uniqExact(test_name) <= {ULTRA_BROAD_REGION_CAP}
              )
            GROUP BY test_name
            ORDER BY (cov_regions * uniqExact(file)) DESC
            LIMIT 4000
            """
            t_ultra = time.monotonic()
            try:
                ultra_raw = cidb.query(ultra_query, log_level="") or ""
                print(f"[find_tests] ultra-broad query: {time.monotonic()-t_ultra:.2f}s, "
                      f"response={len(ultra_raw)} bytes")
            except Exception as e:
                print(f"[find_tests] ultra-broad query failed (non-fatal): {e}")
                ultra_raw = ""

            ultra_tests: dict = {}
            for row in ultra_raw.strip().splitlines():
                parts = row.strip().split("\t", 2)
                if not parts[0]:
                    continue
                cov_regions = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
                files_covered = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
                ultra_tests[parts[0]] = cov_regions * files_covered

            if ultra_tests:
                print(f"[find_tests] ultra-broad: {len(ultra_tests)} tests from "
                      f"regions with rc > {VERY_BROAD_REGION_CAP} (top score={max(ultra_tests.values())})")
                PASS_WEIGHT_ULTRA_BROAD = self.PASS_WEIGHT_BROAD2 / 2
                for filename, line_no in coverage_lines:
                    if not any(filename.startswith(p) for p in COVERAGE_TRACKED_PREFIXES):
                        continue
                    key = (filename, line_no)
                    existing = {e[0] for e in result.get(key, [])}
                    for tname, score in ultra_tests.items():
                        if tname not in existing:
                            effective_width = max(1, BROAD_FALLBACK_WIDTH // max(1, score))
                            result.setdefault(key, []).append(
                                (tname, effective_width, 255, BROAD_TIER2_RC, PASS_WEIGHT_ULTRA_BROAD)
                            )
                    break  # inject into first tracked line only

        # --- Secondary pass: sibling files in the same source directory ----
        # For each changed C++ file under src/, find tests that cover OTHER files
        # in the same directory.  These tests are added as very broad hits
        # (SIBLING_DIR_WIDTH) so they rank below direct hits but are not silently
        # dropped.  This catches, e.g., Arrow-reader tests when the Arrow writer
        # is changed — the reader and writer live side-by-side and a writer change
        # may break reader round-trips that the direct coverage query misses because
        # those tests never call the writer code path directly.
        sibling_tests = self._query_sibling_dir_tests(files_to_lines, result)
        if sibling_tests:
            # Inject into every changed line so the scorer can accumulate width_score.
            # Use the actual region_test_count from the sibling query so heavily-shared
            # sibling tests are penalised the same way as direct broad hits.
            for key in result:
                fname, _ = key
                stored = self._stored_path(fname)
                dir_path = stored.rsplit("/", 1)[0] + "/"
                if not stored.startswith("./src/"):
                    continue
                for t, rc in sibling_tests.get(dir_path, {}).items():
                    # Skip tests already found by the primary query for this line.
                    existing = {e[0] for e in result[key]}
                    if t not in existing:
                        result[key].append((t, self.SIBLING_DIR_WIDTH, 255, max(1, rc), self.PASS_WEIGHT_SIBLING))


        # --- Tertiary pass: file-level expansion for ultra-sparse changed lines ---
        # When ALL coverage regions overlapping a .cpp file's changed lines have
        # rc ≤ SPARSE_FILE_THRESHOLD, the changed code is exercised by at most a
        # handful of tests in nightly runs (e.g. a JSON-column method only called
        # by one specific test).  The primary query finds those tests, but misses
        # related tests that exercise the same class through *other* methods —
        # exactly the gap the old DWARF "any symbol in the TU" approach filled.
        # Fallback: fetch ALL regions in the sparse file with rc ≤ MAX_TESTS_PER_LINE
        # so domain-specific tests (e.g. other JSON wide-part tests) get included.
        SPARSE_FILE_THRESHOLD = 8   # trigger when max primary rc for a file ≤ this
        PASS_WEIGHT_SPARSE_FILE = 0.30  # between direct (1.0) and sibling (0.1)
        SPARSE_FILE_WIDTH = 600        # synthetic width: stronger than sibling (10000)

        # Compute per-file max rc from the Python-filtered result dict (only regions
        # that actually overlapped a changed line, not merely the SQL-fetched neighbourhood).
        # coverage_ranges includes broader SQL hits (e.g. line 1236 rc=76 when the diff
        # only touches 1173-1179), which would spuriously suppress the sparse-file pass.
        file_max_primary_rc: dict = {}
        for (fname, _), pairs in result.items():
            stored = self._stored_path(fname)  # fname from coverage_lines (orig path)
            for _, _, _, region_rc, pw in pairs:
                # Only count DIRECT hits (pw == PASS_WEIGHT_DIRECT) for sparse-file threshold.
                # Hunk-context hits (PASS_WEIGHT_HUNK_CONTEXT) are excluded: a file where
                # the actually-changed lines have rc=0 but nearby context has rc=30 is still
                # "sparse from the changed code's perspective" and benefits from sparse-file
                # expansion.  Including hunk-context hits here would suppress that expansion.
                if pw >= self.PASS_WEIGHT_DIRECT:  # direct-pass entries only
                    if region_rc > file_max_primary_rc.get(stored, 0):
                        file_max_primary_rc[stored] = region_rc

        # Files that appear in the primary SQL response (have coverage in CIDB at all).
        # Used to distinguish "stale / no 3-day data" from "genuinely uncovered file".
        files_in_coverage_ranges = {file_ for file_, *_ in coverage_ranges}

        # files_to_lines keys are already stored-paths (./src/...) — no _stored_path needed.
        sparse_cpp_files = [
            f for f in files_to_lines
            if not f.endswith(".h")
            and f in files_in_coverage_ranges
            and file_max_primary_rc.get(f, 0) <= SPARSE_FILE_THRESHOLD
        ]

        if sparse_cpp_files:
            sparse_conds = " OR ".join(
                f"file = '{self._escape_sql_string(f)}'"
                for f in sparse_cpp_files
            )
            sparse_query = f"""
            SELECT file, line_start, line_end,
                   groupArray(test_name) AS tests,
                   groupArray(min_depth) AS depths,
                   uniqExact(test_name) AS region_test_count
            FROM checks_coverage_lines
            WHERE check_start_time > now() - interval 3 days
              AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              AND notEmpty(test_name)
              AND ({sparse_conds})
            GROUP BY file, line_start, line_end
            HAVING region_test_count <= {self.MAX_TESTS_PER_LINE}
            """
            t_sparse = time.monotonic()
            sparse_elapsed = 0.0
            try:
                sparse_raw = cidb.query(sparse_query, log_level="") or ""
                sparse_elapsed = time.monotonic() - t_sparse
            except Exception as e:
                print(f"[find_tests] sparse-file query failed (non-fatal): {e}")
                sparse_raw = ""

            # Collect per-test best (min width, min depth, min rc) across all regions.
            sparse_file_tests: dict = {}  # test_name -> (width, depth, rc)
            for row in sparse_raw.strip().splitlines():
                if not row:
                    continue
                parts = row.split("\t", 5)
                if len(parts) < 4:
                    continue
                file_, ls_s, le_s, tests_raw = parts[:4]
                depths_raw = parts[4] if len(parts) >= 5 else None
                count_raw  = parts[5] if len(parts) >= 6 else None
                try:
                    tests  = ast.literal_eval(tests_raw.strip())
                    depths = ast.literal_eval(depths_raw.strip()) if depths_raw else None
                    rc     = int(count_raw.strip()) if count_raw else (len(tests) if isinstance(tests, list) else 1)
                    width  = max(1, int(le_s) - int(ls_s) + 1)
                    if not isinstance(tests, list):
                        continue
                    for i, t in enumerate(tests):
                        d = int(depths[i]) if isinstance(depths, list) and i < len(depths) else 255
                        if t not in sparse_file_tests:
                            sparse_file_tests[t] = (width, d, rc)
                        else:
                            ow, od, orc = sparse_file_tests[t]
                            sparse_file_tests[t] = (min(ow, width), min(od, d), min(orc, rc))
                except (ValueError, SyntaxError):
                    pass

            if sparse_file_tests:
                print(
                    f"[find_tests] sparse-file expansion: {len(sparse_file_tests)} tests "
                    f"from {len(sparse_cpp_files)} ultra-sparse file(s) ({sparse_elapsed:.2f}s)"
                )
                # Inject into the first tracked changed line of each sparse file.
                # sparse_cpp_files are already stored-paths; coverage_lines use orig paths
                sparse_stored = set(sparse_cpp_files)
                injected_sparse: set = set()
                for fname, ln in coverage_lines:
                    if not any(fname.startswith(p) for p in COVERAGE_TRACKED_PREFIXES):
                        continue
                    if self._stored_path(fname) not in sparse_stored:
                        continue
                    key = (fname, ln)
                    if key in injected_sparse:
                        continue
                    existing = {e[0] for e in result.get(key, [])}
                    for t, (width, depth, rc) in sparse_file_tests.items():
                        if t not in existing:
                            result.setdefault(key, []).append(
                                (t, SPARSE_FILE_WIDTH, depth, rc, PASS_WEIGHT_SPARSE_FILE)
                            )
                    injected_sparse.add(key)

        # --- Tertiary pass: indirect-call (virtual dispatch) co-occurrence ------
        # Find tests calling the same vtable/function-pointer callees as primary
        # tests. Ranked between direct and sibling hits (INDIRECT_CALL_WIDTH=3000).
        indirect_tests = self._query_indirect_call_tests(result, sparse_cpp_files)
        if indirect_tests:
            for key in result:
                fname, _ = key
                if not any(fname.startswith(p) for p in COVERAGE_TRACKED_PREFIXES):
                    continue
                existing = {e[0] for e in result[key]}
                for t, shared_count in indirect_tests.items():
                    if t not in existing:
                        # Scale effective width inversely with shared_callees so that
                        # tests sharing more callees with the primary set rank higher.
                        # Floor at NARROW_REGION_MAX_LINES+1 to stay in the broad tier.
                        effective_width = max(
                            self.NARROW_REGION_MAX_LINES + 1,
                            self.INDIRECT_CALL_WIDTH // max(1, shared_count),
                        )
                        result[key].append((t, effective_width, 255, 1, self.PASS_WEIGHT_INDIRECT))

        total_unique_tests = len({t for pairs in result.values() for t, *_ in pairs})
        lines_with_tests = sum(1 for (f, _), pairs in result.items() if pairs and any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES))
        print(
            f"[find_tests] done in {time.monotonic()-t0:.2f}s: "
            f"{lines_with_tests}/{len(coverage_lines)} lines matched, "
            f"{total_unique_tests} unique tests selected"
        )

        # Store top broad-tier2 tests (by cov_regions) on self so that
        # get_most_relevant_tests() can guarantee they make it through
        # the MAX_OUTPUT_TESTS cap even if their scores fall below the cutoff.
        n_guarantee = min(100, max(20, len(coverage_lines) // 3))
        self._broad_tier2_guarantee = [
            t
            for t, _ in sorted(broad_tests.items(), key=lambda x: -x[1])
        ][:n_guarantee] if broad_tests else []

        return result

    @staticmethod
    def _extract_domain_keywords(filename: str) -> list:
        """
        Extract domain-specific CamelCase words from a source filename.
        These words are used to filter sibling files so we only expand to
        files in the same functional domain (e.g. "Arrow" for Arrow I/O files)
        rather than all files in the directory.

        Returns a list of significant words (length > 4, not architectural).
        Empty list means no filtering (expand to all sibling files, but this
        is unusual and typically indicates a very generic filename).
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
        words = _re.findall(r"[A-Z]+(?=[A-Z][a-z])|[A-Z][a-z0-9]+|[A-Z]{2,}|[a-z][a-z0-9]+|[A-Z]", base)
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
            "block", "input", "output", "format", "column", "stream",
            "storage", "table", "query", "parser", "writer", "reader",
            "buffer", "default", "base", "impl", "merge", "tree",
            "row", "file", "data", "info", "type", "list", "map",
            "with", "from", "into",
            # MergeTree-specific architectural words (appear in almost every MergeTree file)
            "condition", "granularity", "selector", "partition", "replica",
            "transaction", "virtual", "local", "remote", "range", "level",
            # ClickHouse architectural nouns that appear in many places but are not
            # specific enough to pin to a test domain.
            "handler", "manager", "source", "access", "control",
            "service", "server", "client", "external", "internal",
            "settings", "setting", "config", "context", "result",
            "state", "status", "entry", "record", "update", "create",
        }
        # Allow 3-char all-uppercase acronyms (CSV, ORC, URL, JWT, KQL, etc.) in addition
        # to words ≥ 4 chars.  Generic acronyms like "API", "SQL", "DDL", "DML" are added
        # to COMMON below so they don't generate false matches.
        COMMON_ACRONYMS = {"api", "sql", "ddl", "dml", "ids", "uid", "abi", "cpu", "gpu", "ram",
                           "tcp", "udp", "tls", "ssl", "rpc", "ttl", "log", "tag", "row", "set"}
        specific = [
            w for w in words
            if w.lower() not in COMMON
            and (
                (len(w) >= 4)
                or (len(w) == 3 and w.isupper() and w.lower() not in COMMON_ACRONYMS)
            )
        ]
        return specific

    def _query_indirect_call_tests(self, primary_result: dict, sparse_files: list | None = None) -> dict:
        """
        Tertiary pass: find tests that call the same virtual / function-pointer
        callees as the primary tests (those directly covering changed files).

        Semantic: if primary test A covers changed file F and calls virtual
        callee C (recorded via LLVM value profiling), then any other test B
        that also calls callee C is exercising the same interface — even if B
        never directly calls code in F.  This catches tests that reach changed
        implementations only via vtable dispatch or function pointers.

        Uses only the existing `checks_coverage_indirect_calls` CIDB table;
        no new tables are required.  The join is:
          primary tests → their callee_offsets → other tests with same callees.
        Shared-library callees (operator new, malloc, etc.) are excluded at
        collection time in coverage.cpp, so no extra filter is needed here.

        Degrades gracefully when the table is empty (e.g. first nightly run).
        """
        import time

        # Use only high-confidence primary tests (from regions with
        # <= MAX_TESTS_PER_LINE) for the indirect-call join.  Including all
        # tests from broad regions (e.g. 1000+) would make the IN-list too
        # large for the CIDB HTTP endpoint ("Field value too long").
        assert self.job_type, "_query_indirect_call_tests requires a known job type"
        primary_tests = set()
        seed_rcs: list = []
        SHALLOW_DEPTH = 3  # min_depth ≤ this means the test called the changed function
                           # very sparingly — it specifically targets this code path.
                           # Use as seeds even when rc > MAX_TESTS_PER_LINE.
        SHALLOW_RC_CAP = 2000  # don't use shallow seeds from ultra-broad regions
        for pairs in primary_result.values():
            for entry in pairs:
                t = entry[0]
                depth = entry[2] if len(entry) > 2 else 255
                rc = entry[3] if len(entry) > 3 else 1
                pw = entry[4] if len(entry) > 4 else 1.0
                if pw < self.PASS_WEIGHT_INDIRECT:
                    continue
                is_narrow = rc <= self.MAX_TESTS_PER_LINE
                is_shallow = depth <= SHALLOW_DEPTH and rc <= SHALLOW_RC_CAP
                if is_narrow or is_shallow:
                    primary_tests.add(t)
                    seed_rcs.append(rc)
        if not primary_tests:
            return {}

        # C. Supplement seeds from narrow regions (rc ≤ FILE_SEED_RC) anywhere in
        # the sparse files.  When changed lines have rc=1 (only one test ever ran
        # that path), the existing seeds are too few / too specific to find related
        # tests via Jaccard.  Seeds from other narrow regions in the same file have
        # broader callee coverage and higher overlap with domain-related tests.
        FILE_SEED_RC = 30   # narrower than MAX_TESTS_PER_LINE; avoids pulling in broad seeds
        if sparse_files:
            from ci.praktika.cidb import CIDB
            from ci.praktika.settings import Settings
            _cidb = CIDB(url=_PUBLIC_CIDB_URL, user="play", passwd="")
            # sparse_files are already stored-paths (./src/...)
            sparse_conds = " OR ".join(
                f"file = '{self._escape_sql_string(f)}'"
                for f in sparse_files
            )
            seed_query = f"""
            SELECT file, line_start, line_end,
                   groupArray(test_name) AS tests,
                   uniqExact(test_name) AS rc
            FROM checks_coverage_lines
            WHERE check_start_time > now() - interval 3 days
              AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              AND notEmpty(test_name)
              AND ({sparse_conds})
            GROUP BY file, line_start, line_end
            HAVING rc <= {FILE_SEED_RC}
            """
            try:
                seed_raw = _cidb.query(seed_query, log_level="") or ""
                extra_seeds = 0
                for row in seed_raw.strip().splitlines():
                    parts = row.split("\t", 4)
                    if len(parts) < 4:
                        continue
                    try:
                        tests = ast.literal_eval(parts[3].strip())
                        rc    = int(parts[4].strip()) if len(parts) >= 5 else len(tests)
                        if isinstance(tests, list):
                            for t in tests:
                                if t not in primary_tests:
                                    primary_tests.add(t)
                                    seed_rcs.append(rc)
                                    extra_seeds += 1
                    except (ValueError, SyntaxError):
                        pass
                if extra_seeds:
                    print(f"[find_tests] indirect seeds: +{extra_seeds} from narrow file regions")
            except Exception as e:
                print(f"[find_tests] seed enrichment query failed (non-fatal): {e}")

        # B. Adaptive Jaccard threshold.
        # Only lower the threshold for truly sparse code (rc < 20) where the seed
        # set is tiny and even 15% callee overlap is a meaningful signal.
        # For rc ≥ 20 we already have enough seeds to enforce the full 70% threshold
        # — loosening it further admits tests that share only generic callees
        # (memory allocation, string ops) and pollutes results with non-domain tests.
        #   rc=1   → 15%  (only 1 seed: anything sharing callees is relevant)
        #   rc=10  → 43%
        #   rc=20+ → 70%  (enough seeds: enforce strict domain match)
        min_seed_rc = min(seed_rcs) if seed_rcs else 200
        JACCARD_MIN_PCT = max(15, min(70, 15 + int(55 * min(min_seed_rc, 20) / 20)))
        # Adaptive LIMIT: fewer indirect tests when we already have many direct seeds.
        # With many seeds the callee union is large and 70% Jaccard admits too many
        # loosely-related tests.  Cap inversely with seed count so that:
        #   n_seeds=1  → limit=200 (need many indirect to compensate)
        #   n_seeds=10 → limit=100
        #   n_seeds=50 → limit=20 (direct evidence is already rich)
        n_seeds = len(primary_tests)
        INDIRECT_LIMIT = max(20, min(200, int(200 / max(1, n_seeds / 5))))

        # Cap at 500 tests to prevent HTTP field-length errors.
        if len(primary_tests) > 500:
            primary_tests = set(sorted(primary_tests)[:500])

        escaped_primary = ", ".join(
            f"'{self._escape_sql_string(t)}'" for t in sorted(primary_tests)
        )

        # Self-join on callee_offset ranked by Jaccard-like specificity.
        #
        # Problem with raw shared_callees ordering: infrastructure tests (filesystem
        # cache, S3) rank highest because they share many I/O virtual-dispatch callees
        # with any MergeTree-reading primary test, even if they're completely unrelated
        # to the changed code domain.
        #
        # Fix: rank by  shared / secondary_test_total_specific_callees  (Jaccard
        # fraction).  A test whose callee set is mostly overlapping with the primary
        # set is specifically exercising the same domain; a filesystem-cache test whose
        # 200 specific callees all happen to be in the primary set gets lower rank than
        # a DDL test whose 50 specific callees are 90% shared with the primary set.
        #
        # Additional guards:
        #   MIN_SHARED       — require at least this many shared callees (eliminates
        #                      accidental 1-2 callee overlaps).
        #   MIN_SECONDARY    — secondary test must have at least this many specific
        #                      callees; avoids 100%-Jaccard artifacts from tiny sets.
        #   MAX_CALLEE_COUNT — exclude globally-ubiquitous callees (logging, malloc).
        MAX_CALLEE_TEST_COUNT = 300  # callees in >= this many tests are ubiquitous
        # Thresholds scale with the primary set size so that small primary sets
        # (e.g. 3 tests for a focused single-file PR) still find results while
        # large primary sets use stricter filters to suppress infrastructure noise.
        n_primary = len(primary_tests)
        MIN_SHARED    = max(1, min(10, n_primary // 5))   # 2 for tiny, 10 for large
        MIN_SECONDARY = max(5, min(50, n_primary * 3))   # 15 for tiny, 50 for large
        query = f"""
        SELECT
            ic2.test_name,
            count(DISTINCT ic1.callee_offset) AS shared_callees,
            ic2_tot.tot_callees,
            count(DISTINCT ic1.callee_offset) * 100.0 / ic2_tot.tot_callees AS jaccard_pct
        FROM checks_coverage_indirect_calls ic1
        JOIN checks_coverage_indirect_calls ic2 ON ic1.callee_offset = ic2.callee_offset
        JOIN (
            -- Total number of specific (non-ubiquitous) callees each secondary test
            -- uses.  Used as the Jaccard denominator.
            SELECT test_name, count(DISTINCT callee_offset) AS tot_callees
            FROM checks_coverage_indirect_calls
            WHERE check_start_time > now() - interval 3 days
              AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              AND callee_offset IN (
                  SELECT callee_offset
                  FROM checks_coverage_indirect_calls
                  WHERE check_start_time > now() - interval 3 days
                    AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                  GROUP BY callee_offset
                  HAVING uniqExact(test_name) < {MAX_CALLEE_TEST_COUNT}
              )
            GROUP BY test_name
            HAVING tot_callees >= {MIN_SECONDARY}
        ) ic2_tot ON ic2.test_name = ic2_tot.test_name
        WHERE ic1.check_start_time > now() - interval 3 days
          AND ic2.check_start_time > now() - interval 3 days
          AND ic1.check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND ic2.check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND ic1.test_name IN ({escaped_primary})
          AND ic2.test_name NOT IN ({escaped_primary})
          AND ic1.callee_offset IN (
              SELECT callee_offset
              FROM checks_coverage_indirect_calls
              WHERE check_start_time > now() - interval 3 days
                AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
              GROUP BY callee_offset
              HAVING uniqExact(test_name) < {MAX_CALLEE_TEST_COUNT}
          )
        GROUP BY ic2.test_name, ic2_tot.tot_callees
        HAVING shared_callees >= {MIN_SHARED}
           AND count(DISTINCT ic1.callee_offset) * 100.0 / ic2_tot.tot_callees >= {JACCARD_MIN_PCT:.1f}
        ORDER BY jaccard_pct DESC, shared_callees DESC
        LIMIT {INDIRECT_LIMIT}
        """

        try:
            from ci.praktika.cidb import CIDB
            from ci.praktika.settings import Settings
            cidb = CIDB(url=_PUBLIC_CIDB_URL, user="play", passwd="")
            t0 = time.monotonic()
            raw = cidb.query(query, log_level="") or ""
            elapsed = time.monotonic() - t0
        except Exception as e:
            print(f"[find_tests] indirect-call query failed (non-fatal): {e}")
            return {}

        # Parse TSV: test_name \t shared_callees \t tot_callees \t jaccard_pct
        result_map: dict = {}
        for row in raw.strip().splitlines():
            parts = row.split("\t")
            if len(parts) >= 2 and parts[0].strip():
                try:
                    result_map[parts[0].strip()] = int(parts[1].strip())
                except ValueError:
                    pass
        if not result_map:
            return {}

        # Extract top Jaccard score from the raw output for logging
        top_jaccard = 0.0
        for row in raw.strip().splitlines():
            parts = row.split("\t")
            if len(parts) >= 4:
                try:
                    top_jaccard = max(top_jaccard, float(parts[3].strip()))
                except ValueError:
                    pass
        print(
            f"[find_tests] indirect-call query: {elapsed:.2f}s, "
            f"{len(result_map)} additional test candidates via callee co-occurrence "
            f"(top jaccard={top_jaccard:.0f}%, top shared={max(result_map.values())})"
        )
        return result_map

    def _query_sibling_dir_tests(self, files_to_lines: dict, primary_result: dict) -> dict:
        """
        Secondary pass: find tests that are likely related to the changed files
        by co-coverage proximity.

        For each changed C++ source file under src/ we:
          1. Identify which sibling files (same directory, different name) the
             PRIMARY tests also cover.  These are files with a natural code
             relationship to the changed file (e.g. Arrow reader ↔ writer).
          2. Find tests that cover those same sibling files but were NOT already
             selected by the primary query.  These are tests that exercise the
             same functional area from a different entry point.

        Because we only expand through files that the primary tests already touch,
        we avoid the false positives that come from querying the whole directory
        (which would mix in unrelated format tests, HTTP tests, etc.).

        Returns {dir_path -> {test_name -> SIBLING_RC}}.
        """
        import time

        assert self.job_type, "_query_sibling_dir_tests requires a known job type"
        # Collect source directories of changed C++ files.
        src_dirs: dict = {}  # dir_path -> list of changed files in that dir
        for f in files_to_lines:
            if f.startswith("./src/") and (f.endswith(".cpp") or f.endswith(".h")):
                dir_path = f.rsplit("/", 1)[0] + "/"
                src_dirs.setdefault(dir_path, []).append(f)

        if not src_dirs:
            return {}

        # Collect high-confidence primary tests (from narrow or moderately-broad
        # regions) for the sibling-dir expansion.  Using all primary tests
        # (including those from very broad regions) would make the IN-list
        # too large for the CIDB HTTP endpoint.
        primary_tests = set()
        for pairs in primary_result.values():
            for entry in pairs:
                t = entry[0]
                rc = entry[3] if len(entry) > 3 else 1
                pw = entry[4] if len(entry) > 4 else 1.0
                if rc <= self.MAX_TESTS_PER_LINE and pw >= self.PASS_WEIGHT_INDIRECT:
                    primary_tests.add(t)
        if not primary_tests:
            return {}

        # Cap at 500 tests to prevent HTTP field-length errors.
        if len(primary_tests) > 500:
            primary_tests = set(sorted(primary_tests)[:500])

        changed_files = set(files_to_lines.keys())
        escaped_primary = ", ".join(
            f"'{self._escape_sql_string(t)}'" for t in sorted(primary_tests)
        )
        dir_conds = " OR ".join(
            f"file LIKE '{self._escape_sql_string(d)}%'"
            for d in sorted(src_dirs)
        )
        not_changed = " AND ".join(
            f"file != '{self._escape_sql_string(f)}'"
            for f in sorted(changed_files)
        )

        # Extract domain keywords from changed C++ SOURCE filenames only (not
        # test files, which would add misleading keywords like "uuid" or "parquet").
        # E.g. "Arrow" from CHColumnToArrowColumn.cpp ensures we only look at
        # Arrow-related siblings, not every file in the directory.
        all_keywords: list = []
        for f in sorted(changed_files):
            if f.startswith("./src/") and (f.endswith(".cpp") or f.endswith(".h")):
                all_keywords.extend(self._extract_domain_keywords(f.split("/")[-1]))
        # Deduplicate, keep unique keywords only (not repeated across changed files)
        unique_kws = list(dict.fromkeys(all_keywords))

        # Build keyword filter for sibling filenames.
        # Use OR across keywords so that compound-named files (e.g. "DistributedSink")
        # can find siblings matching EITHER component ("Distributed" OR "Sink").
        # Previously AND was used but it was too restrictive: when a PR changes
        # DistributedSink.cpp (keywords: Distributed, Sink), the AND condition
        # "file LIKE '%Distributed%' AND file LIKE '%Sink%'" only matched
        # DistributedSink.cpp itself (the changed file), yielding zero sibling results.
        # With OR, related sibling files (DistributedAsyncInsert*.cpp etc.) are found.
        #
        # False positive risk is mitigated by:
        # 1. MAX_SIBLING_FILE_TESTS cap (200): broad infrastructure files excluded.
        # 2. Inner subquery: sibling file must be covered by >= min_sibling_coverage
        #    primary tests, so loosely-related files not exercised by the PR's primary
        #    tests are naturally excluded.
        if unique_kws:
            kws = unique_kws[:4]  # cap to avoid overly long queries
            # OR: sibling file must contain at least one keyword.
            kw_cond = " OR ".join(
                f"file LIKE '%{self._escape_sql_string(kw)}%'"
                for kw in kws
            )
            sibling_file_filter = f"AND ({kw_cond})"
        else:
            # No specific keywords — fall back to all sibling files (rare)
            sibling_file_filter = ""

        # Step 1 + 2 combined: find tests covering sibling files in the same
        # functional domain as the changed file.  The inner SELECT identifies
        # sibling files with the right domain keywords that the primary tests
        # already touch; the outer SELECT finds new tests for those same files.
        #
        # Two filters prevent broad infrastructure files from flooding the candidates:
        #
        # 1. INNER subquery min coverage: the sibling file must be covered by at
        #    least MIN_SIBLING_COVERAGE primary tests.  This excludes files that are
        #    only incidentally touched by primary tests (e.g. AggregatedDataVariants.cpp
        #    being exercised by Variant tests through GROUP BY operations).
        #
        # 2. Global test count cap (NOT IN subquery): exclude sibling files that are
        #    covered by more than MAX_SIBLING_TESTS distinct tests globally.  Very
        #    broadly-covered files (like AggregatedDataVariants.cpp with 3400 tests,
        #    or RewriteCountVariantsVisitor.cpp with 4250 tests) are infrastructure —
        #    finding their tests adds noise rather than signal.  The threshold matches
        #    MAX_TESTS_PER_LINE so we consistently exclude "too common" files.
        MAX_SIBLING_FILE_TESTS = self.MAX_TESTS_PER_LINE  # same cap as direct coverage
        n_primary = len(primary_tests)
        min_sibling_coverage = max(2, n_primary // 5)
        query = f"""
        SELECT DISTINCT test_name
        FROM checks_coverage_lines
        WHERE check_start_time > now() - interval 3 days
          AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
          AND notEmpty(test_name)
          AND test_name NOT IN ({escaped_primary})
          AND ({dir_conds})
          AND ({not_changed})
          {sibling_file_filter}
          AND file NOT IN (
              SELECT file
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND check_name LIKE '{self._escape_sql_string(self.job_type)}%'
                AND ({dir_conds})
                {sibling_file_filter}
              GROUP BY file
              HAVING uniqExact(test_name) > {MAX_SIBLING_FILE_TESTS}
          )
          AND file IN (
              SELECT file
              FROM checks_coverage_lines
              WHERE check_start_time > now() - interval 3 days
                AND test_name IN ({escaped_primary})
                AND ({dir_conds})
                AND ({not_changed})
                {sibling_file_filter}
              GROUP BY file
              HAVING uniqExact(test_name) >= {min_sibling_coverage}
          )
        LIMIT 200
        """

        try:
            from ci.praktika.cidb import CIDB
            from ci.praktika.settings import Settings
            cidb = CIDB(url=_PUBLIC_CIDB_URL, user="play", passwd="")
            t0 = time.monotonic()
            raw = cidb.query(query, log_level="") or ""
            print(
                f"[find_tests] sibling-dir query: {time.monotonic()-t0:.2f}s, "
                f"response={len(raw)} bytes"
            )
        except Exception as e:
            print(f"[find_tests] sibling-dir query failed (non-fatal): {e}")
            return {}

        # Parse TSV: test_name (single column).
        SIBLING_RC = 300
        sibling_test_names: dict = {}
        for row in raw.strip().splitlines():
            test_name = row.strip()
            if test_name:
                sibling_test_names[test_name] = SIBLING_RC

        total = len(sibling_test_names)
        print(f"[find_tests] sibling-dir: {total} additional test candidates")
        # Return the same set for every changed src dir.
        return {d: dict(sibling_test_names) for d in src_dirs}

    # Synthetic width for keyword-fallback tests (broader than sibling-dir tests
    # since we have no coverage signal at all — just filename matching).
    KEYWORD_FALLBACK_WIDTH = 30000

    def _get_keyword_fallback_tests(self, changed_src_files: list) -> list:
        """
        Last-resort fallback: when coverage gives zero results for one or more C++
        source files, try to find stateless tests whose *filename* contains domain
        keywords extracted from the changed source files.

        This handles cases like `Parquet/Decoding.cpp` where the file may not appear
        in the coverage database (e.g. the changed code path is rarely executed by
        existing tests), but there clearly exist stateless tests named
        `*parquet*.sql` / `*parquet*.sh` that should be run.

        Returns a list of `(test_name, width, depth, region_test_count)` tuples
        using KEYWORD_FALLBACK_WIDTH so they always rank below any direct or
        sibling hit.
        """
        import glob as _glob

        if not changed_src_files:
            return []

        # Collect domain keywords from ALL changed source files.
        # Include the *parent directory name* in keyword extraction so that
        # generic filenames like `Decoding.cpp` inside `Parquet/` still yield
        # "Parquet" as a keyword and match `*parquet*.sh` tests.
        all_keywords: list = []
        # Top-level src subdirectories whose names are too generic to use as
        # test-name keywords (they appear in thousands of test files).
        GENERIC_DIRS = frozenset({
            "src", "programs", "utils", "base",
            "Storages", "Interpreters", "Processors", "Functions",
            "Common", "Server", "Parsers", "Analyzer", "Formats",
            "Access", "IO", "Disks", "Columns", "DataTypes", "Core",
            "Databases", "Backups", "Coordination", "Client",
            "Daemon", "Compression", "AggregateFunctions",
            "TableFunctions", "Dictionaries", "QueryPipeline",
            # Sub-directories that are generic groupings (not domain names)
            "Impl", "Sources", "Transforms", "Sinks",
            "Utils", "Tests", "tests",
        })

        dir_keywords: list = []  # keywords from path components (directory names)
        for f in changed_src_files:
            # Keywords from basename (e.g. "CHColumnToArrowColumn.cpp" → "Arrow")
            kws = self._extract_domain_keywords(f.split("/")[-1])
            all_keywords.extend(kws)
            # Keywords from ALL path components that are specific enough.
            # e.g. src/Processors/Formats/Impl/Parquet/Decoding.cpp → "Parquet" from the
            # 4th component (skipping "src", "Processors", "Formats", "Impl").
            parts = f.replace("\\", "/").split("/")
            for part in parts[:-1]:  # all path components except the filename
                if part in GENERIC_DIRS:
                    continue
                part_kws = self._extract_domain_keywords(part + ".cpp")
                for pk in part_kws:
                    if pk not in dir_keywords:
                        dir_keywords.append(pk)
        # Directory keywords are prepended so they are considered first (they tend to
        # be better test-name prefixes than generic filename decompositions).
        for dk in dir_keywords:
            if dk not in all_keywords:
                all_keywords.insert(0, dk)
        unique_kws = list(dict.fromkeys(all_keywords))

        if not unique_kws:
            return []

        # Find the 0_stateless test directory relative to the repo root.
        test_dir = Path("tests/queries/0_stateless")
        if not test_dir.is_dir():
            return []

        # Pre-build list of (sql+sh) test filenames for quick iteration.
        all_test_files = [
            f.name for f in test_dir.iterdir()
            if f.name.endswith(".sql") or f.name.endswith(".sh")
        ]

        # For each candidate keyword, count how many tests it matches.
        # Keywords matching too many tests (too generic) or zero tests
        # (no signal) are discarded.
        # We use two tiers:
        #   Specific tier (1–100 hits): high confidence, domain-specific keyword.
        #     At 100 we include domain words like "Arrow" (54), "Parquet" (99),
        #     "text_index" (86), "Variant" (64) while still excluding broad words.
        #   Broad tier  (101–200 hits): lower confidence, include only if no
        #                               specific keyword is available.
        SPECIFIC_MAX = 100
        BROAD_MAX = 200

        def count_hits(kw_lower: str) -> int:
            return sum(1 for f in all_test_files if kw_lower in f.lower())

        specific_kws = []  # (hits, -len, kw) — few hits, long name preferred
        broad_kws = []
        for kw in unique_kws:
            if len(kw) < 4:
                continue
            hits = count_hits(kw.lower())
            if 1 <= hits <= SPECIFIC_MAX:
                specific_kws.append((hits, -len(kw), kw))
            elif SPECIFIC_MAX < hits <= BROAD_MAX:
                broad_kws.append((hits, -len(kw), kw))
        specific_kws.sort()
        broad_kws.sort()

        # Strategy: prefer directory-origin keywords (they directly name the domain,
        # e.g. "Parquet" from src/.../Parquet/Decoding.cpp).
        # Directory keywords come first in unique_kws (prepended above).
        # We use at most one keyword to keep the test set focused.
        dir_kw_set = set(dir_keywords)

        # Split into directory-origin and filename-origin, each sorted by hit count.
        all_ranked = specific_kws + [(h, l, kw) for h, l, kw in broad_kws]
        all_ranked.sort()  # fewest hits first, longer name preferred

        dir_ranked  = [(h, l, kw) for h, l, kw in all_ranked if kw in  dir_kw_set]
        file_ranked = [(h, l, kw) for h, l, kw in all_ranked if kw not in dir_kw_set]

        # Pre-count actual test-file hits so we can detect zero-hit keywords and skip them.
        def has_hits(kw: str) -> bool:
            kw_lower = kw.lower()
            return any(kw_lower in f.lower() for f in all_test_files)

        candidate_kws: list = []
        if dir_ranked:
            # Use the best directory keyword.  Skip any that match zero tests (e.g.
            # "Coordination" for KeeperStorage.cpp — no test names contain "coordination").
            # Fall back through the list until one has hits; if none do, continue to
            # file_ranked below.
            usable_dir = [(h, l, kw) for h, l, kw in dir_ranked if has_hits(kw)]
            if usable_dir:
                candidate_kws = [usable_dir[0][2]]
                # Also add a specific file keyword to narrow results further.
                if file_ranked and file_ranked[0][0] <= SPECIFIC_MAX:
                    candidate_kws.append(file_ranked[0][2])
            else:
                # All directory keywords have 0 test hits; fall through to file_ranked.
                dir_ranked = []

        if not dir_ranked:
            if not file_ranked:
                return []
            # No useful directory keyword; use the most specific file keyword available.
            # Prefer the specific tier (≤SPECIFIC_MAX), but fall back to the broad tier
            # (≤BROAD_MAX) when nothing specific exists — e.g. "Keeper" (156 hits) is
            # still a meaningful domain signal for KeeperStorage.cpp changes.
            file_specific = [x for x in file_ranked if x[0] <= SPECIFIC_MAX]
            if file_specific:
                candidate_kws = [file_specific[0][2]]
            elif broad_kws:
                candidate_kws = [broad_kws[0][2]]
            else:
                return []

        # Collect test names matching any of the selected keywords.
        # Track the best (most specific) keyword that matched each test so we
        # can weight by keyword specificity: rare keywords → narrow effective
        # width → higher score.  Among ties, prefer newer tests (higher number).
        #
        # Scoring:  score = PASS_WEIGHT_KEYWORD / (KEYWORD_FALLBACK_WIDTH × kw_hits)
        #   kw_hits=1  → width=50000   score=1e-6  (very specific)
        #   kw_hits=10 → width=500000  score=1e-7
        #   kw_hits=100→ width=5000000 score=1e-8  ≈ MIN_SCORE (barely survives)
        # Tests from broad keywords (hits>100) score below MIN_SCORE and are
        # dropped at the ranking stage — no hard per-keyword filter needed.
        MAX_KEYWORD_TESTS = 50   # hard cap: keyword signal is weak, keep it tight
        matched_tests: dict = {}  # normalised_name → (kw_hits, raw_name)
        for kw in candidate_kws:
            kw_lower = kw.lower()
            kw_hits = count_hits(kw_lower)
            for fname in all_test_files:
                if kw_lower in fname.lower():
                    base = os.path.splitext(fname)[0]
                    tname = base + "."
                    if tname not in matched_tests or kw_hits < matched_tests[tname][0]:
                        matched_tests[tname] = (kw_hits, kw)

        if not matched_tests:
            return []

        # Sort: most-specific keyword first (fewest hits), then newest test first
        # (higher 5-digit prefix tends to be closer to the recently changed code).
        def _kw_sort(item):
            tname, (kw_hits, _kw) = item
            m = re.match(r'(\d{5})', os.path.basename(tname).lstrip('./'))
            test_num = int(m.group(1)) if m else 0
            return (kw_hits, -test_num)

        sorted_tests = sorted(matched_tests.items(), key=_kw_sort)[:MAX_KEYWORD_TESTS]
        kws_used = sorted({kw for _, (_, kw) in sorted_tests})
        print(
            f"[find_tests] keyword-fallback: {len(sorted_tests)} tests "
            f"matching keywords {kws_used} (from {len(matched_tests)} candidates)"
        )
        return [
            (tname, self.KEYWORD_FALLBACK_WIDTH * kw_hits, 255, 1, self.PASS_WEIGHT_KEYWORD)
            for tname, (kw_hits, _kw) in sorted_tests
        ]

    def get_changed_or_new_tests_with_info(self):
        tests = sorted(self.get_changed_tests())
        info = f"Found {len(tests)} changed or new tests:\n"
        for test in tests[:200]:
            info += f" - {test}\n"
        return tests, Result(
            name="tests that were changed or added",
            status=Result.StatusExtended.OK,
            info=info,
        )

    def get_previously_failed_tests_with_info(self):
        try:
            tests = self.get_previously_failed_tests()
        except Exception as e:
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
            status=Result.StatusExtended.OK,
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
            if line.startswith("--- "):
                in_hunk = False
            elif line.startswith("+++ b/"):
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
                else:
                    old_line += 1  # context line
        return list(changed)

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
            if line.startswith("--- "):
                pass
            elif line.startswith("+++ b/"):
                current_file = line[6:]
            elif line.startswith("@@ ") and current_file:
                # @@ -old_start,old_count +new_start,new_count @@
                m = re.search(r"-(\d+)(?:,(\d+))?", line)
                if m:
                    start = int(m.group(1))
                    count = int(m.group(2)) if m.group(2) is not None else 1
                    end = start + count - 1
                    hunks.setdefault(current_file, []).append((start, end))
        return hunks

    def get_diff_text(self) -> str:
        """Fetch the PR diff text (cached on self._diff_text after first call).

        CI containers have no .git directory. Use the GitHub API via curl.
        For public repos no auth is needed; for private repos GITHUB_TOKEN is used.
        """
        if hasattr(self, '_diff_text') and self._diff_text is not None:
            return self._diff_text
        assert self.info.pr_number > 0, "Diff fetching applicable for PRs only"
        repo = self.info.repo_name or "ClickHouse/ClickHouse"
        if self.info.is_local_run:
            self._diff_text = Shell.get_output(
                f"gh pr diff {self.info.pr_number} --repo {repo}"
            )
        else:
            # Use GitHub REST API to get the diff. Works for both public and private
            # repos; private repos require GITHUB_TOKEN (available in Actions env).
            token = os.environ.get("GITHUB_TOKEN", "")
            auth = f'-H "Authorization: Bearer {token}"' if token else ""
            self._diff_text = Shell.get_output(
                f"curl -sSf {auth} "
                f"-H 'Accept: application/vnd.github.v3.diff' "
                f"'https://api.github.com/repos/{repo}/pulls/{self.info.pr_number}'"
            )
        return self._diff_text

    def get_changed_lines_from_diff(self):
        """
        Return changed lines from the PR diff.
        In CI fetches the diff from GitHub API; for local runs uses `gh pr diff`.
        """
        assert self.info.pr_number > 0, "Find tests by diff applicable for PRs only"
        return self._parse_diff_lines(self.get_diff_text())

    # min_depth stores the raw entry-counter call count (capped at 254; 255 = not tracked).
    # A low call count means the function was called rarely during the test → more specific.
    # Tests where a function was called ≤ this many times get the "direct" tier bonus.
    DIRECT_CALL_MAX_DEPTH = 10

    def get_most_relevant_tests(self):
        """
        1. Gets changed lines from the PR diff.
        2. Queries `checks_coverage_lines` for tests covering those lines.
        3. Ranks tests by a composite score with three tiers:
             - Tier A (best):  narrow-region hit AND shallow call depth (≤ DIRECT_CALL_MAX_DEPTH)
             - Tier B:         narrow-region hit, deep call path
             - Tier C (worst): only broad-region hits
           Within each tier tests are ordered by width_score = sum(1/region_width),
           which rewards tests that cover many of the changed lines through narrow regions.
           The min_depth for each test is the shallowest call depth seen across all
           coverage entries for that test (255 = depth not tracked; treated as deep).
        4. Returns the ranked list and a `Result` with info about the findings.
        """
        changed_lines = self.get_changed_lines_from_diff()
        # Also parse hunk boundaries so the CIDB query covers context lines within
        # each hunk (not just the actually-changed lines).  See _parse_diff_hunk_ranges.
        hunk_ranges = self._parse_diff_hunk_ranges(self.get_diff_text())
        line_to_tests = self.get_tests_by_changed_lines(changed_lines,
                                                        hunk_ranges=hunk_ranges)

        # Keyword-based fallback: if no coverage results were found for any
        # C++ source file, try to find tests by matching the source filename
        # against stateless test names.  This catches files that are rarely
        # (or never) reached by the current coverage suite — e.g.
        # Parquet/Decoding.cpp when the change is a new edge-case code path.
        # The fallback is only activated when the primary coverage query returned
        # zero tests for a C++ file so it does not dilute high-confidence hits.
        COVERAGE_TRACKED_PREFIXES = ("src/", "programs/", "utils/", "base/")
        cpp_with_zero_coverage = [
            f for f, ln in changed_lines
            if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
            and (f.endswith(".cpp") or f.endswith(".h"))
            and not any(pairs for (ff, _), pairs in line_to_tests.items() if ff == f)
        ]
        # Deduplicate file list.
        cpp_with_zero_coverage = list(dict.fromkeys(cpp_with_zero_coverage))
        if cpp_with_zero_coverage:
            # Run per-file so each file's domain keywords are matched independently.
            seen_fb: set = set()
            fallback_quads: list = []
            for f in cpp_with_zero_coverage:
                for q in self._get_keyword_fallback_tests([f]):
                    if q[0] not in seen_fb:
                        seen_fb.add(q[0]); fallback_quads.append(q)
            if fallback_quads:
                # Inject fallback tests into the first line of each zero-coverage
                # C++ file so the scorer sees them without duplication.
                injected: set = set()
                for f in cpp_with_zero_coverage:
                    for (ff, ln), pairs in line_to_tests.items():
                        if ff == f and (ff, ln) not in injected:
                            pairs.extend(fallback_quads)
                            injected.add((ff, ln))
                            break

        # Supplementary keyword pass: even for C++ files WITH direct coverage,
        # add tests whose *filename* contains domain keywords from the changed
        # file.  This catches broad regression tests (e.g. 01273_arrow.sh for
        # CHColumnToArrowColumn.cpp) that exercise the same domain through
        # higher-level call paths not captured in line coverage — the same tests
        # the old symbol-based algo found via checks_coverage_inverted.
        # Uses PASS_WEIGHT_KEYWORD (lowest weight) so they rank below all
        # coverage-backed hits but are still present for the scorer.
        cpp_with_coverage = list(dict.fromkeys(
            f for f, ln in changed_lines
            if any(f.startswith(p) for p in COVERAGE_TRACKED_PREFIXES)
            and (f.endswith(".cpp") or f.endswith(".h"))
            and f not in cpp_with_zero_coverage
            and any(pairs for (ff, _), pairs in line_to_tests.items() if ff == f)
        ))
        # Always run the supplementary keyword pass: keyword tests get PASS_WEIGHT_KEYWORD
        # (the lowest weight), so they rank below all coverage-backed hits but remain
        # present in the scoring pool.  This ensures domain-specific tests (e.g.
        # 03444_analyzer_resolve_alias_columns for QueryAnalyzer.cpp) are never
        # silently dropped simply because coverage produced many generic candidates.

        if cpp_with_coverage:
            # Run keyword fallback per changed file so each file's domain keywords
            # are matched independently.  Calling with all files at once collapses
            # to a single "best" keyword and silently drops domains from other files
            # (e.g. for a PR touching ArrowColumnToCHColumn.cpp + MsgPackRowInputFormat.cpp
            # the combined call picks "Pack" as more specific, missing all Arrow tests).
            seen_supplement: set = set()
            all_supplement_quads: list = []
            for f in cpp_with_coverage:
                per_file_quads = self._get_keyword_fallback_tests([f])
                for q in per_file_quads:
                    if q[0] not in seen_supplement:
                        seen_supplement.add(q[0])
                        all_supplement_quads.append(q)
            if all_supplement_quads:
                injected_s: set = set()
                for f in cpp_with_coverage:
                    for (ff, ln), pairs in line_to_tests.items():
                        if ff == f and (ff, ln) not in injected_s:
                            existing = {q[0] for q in pairs}
                            pairs.extend(
                                q for q in all_supplement_quads if q[0] not in existing
                            )
                            injected_s.add((ff, ln))
                            break

            # Store keyword tests for the guarantee injection after ranking.
            self._keyword_guarantee = [q[0] for q in all_supplement_quads]

        # Accumulate per-test scores across all changed lines.
        # line_to_tests values are lists of
        #   (test_name, region_width, min_depth, region_test_count, pass_weight).
        #
        # Scoring combines four signals:
        #   pass_weight   — per-pass multiplier (1.0 direct, 0.3 indirect, 0.1 sibling, 0.05 keyword)
        #   width         — narrow regions (few lines) are more precise → weight 1/width
        #   region_test_count — regions covered by few tests are more specific → weight 1/region_test_count
        #   min_depth     — low call count means this test specifically exercised this path
        #
        # Final score = sum(pass_weight / (width × region_test_count)) across all matched changed lines.
        width_score: dict = {}    # test -> sum(pass_weight/(width×region_test_count))
        has_narrow_hit: dict = {} # test -> bool: any covering region is narrow AND exclusive
        min_depth_seen: dict = {} # test -> minimum call count across all hits
        for quads in line_to_tests.values():
            for t, width, depth, region_test_count, pass_weight in quads:
                rc = max(1, region_test_count)
                width_score[t] = width_score.get(t, 0.0) + pass_weight / (width * rc)
                has_narrow_hit[t] = has_narrow_hit.get(t, False) or (
                    width <= self.NARROW_REGION_MAX_LINES
                )
                if depth < min_depth_seen.get(t, 256):
                    min_depth_seen[t] = depth

        def sort_key(t):
            narrow = has_narrow_hit[t]
            depth = min_depth_seen.get(t, 255)
            direct = narrow and depth <= self.DIRECT_CALL_MAX_DEPTH
            # Tier A: direct narrow hit → 0; Tier B: indirect narrow → 1; Tier C: broad → 2
            tier = 0 if direct else (1 if narrow else 2)
            return (tier, -width_score[t])

        # Sort: tier first (A < B < C), then by width score descending within tier.
        # Use score-based filtering instead of a hard count cap.  The old
        # `[:1000]` cut threw away low-scoring but genuinely relevant tests
        # from infrastructure files (Context.cpp, ProcessList.cpp) whose
        # regions have high region_test_count and therefore low per-line
        # scores.  A minimum score threshold keeps the result set bounded
        # without an arbitrary count limit.
        MIN_SCORE = 1e-8        # floor: tests scoring below this have negligible signal
        MAX_OUTPUT_TESTS = 250   # hard cap: targeted runs must stay focused
        # Dynamic ratio floor: drop tests whose score is more than MAX_SCORE_RATIO times
        # weaker than the best evidence.  This supersedes per-pass suppression guards —
        # when strong direct hits exist (e.g. rc=37 on a SAMPLE-specific line, score=0.027)
        # weak signals like broad-tier2 (score=8e-7, ratio=34000x) or distant indirect-call
        # results are naturally excluded without any explicit per-pass logic.
        # A ratio of 1000 covers ~two jumps in signal quality (direct→indirect→keyword)
        # while keeping genuinely related tests from weaker passes when direct is sparse.
        MAX_SCORE_RATIO = 3000
        # Cap the floor so that rc=1 changed lines (top_score=1.0) don't push
        # effective_min to 1e-3 and cut indirect / sparse-file expansion results.
        # Equivalent to treating any top_score above 1/1000 the same as 1/1000.
        # Using 1e-6 recovers tests that cover changed .h files via non-overlapping
        # regions (header expansion) at slightly higher rc values.
        # score = 1.0/(SIBLING_DIR_WIDTH × rc) = 1.0/(10000 × rc)
        # rc=10: score=1e-5, rc=100: score=1e-6 — both now pass the filter.
        MAX_EFFECTIVE_MIN = 1e-6
        # Compute top_score from direct-pass entries ONLY (pw >= PASS_WEIGHT_DIRECT = 1.0).
        # Hunk-context hits (PASS_WEIGHT_HUNK_CONTEXT = 0.35) are excluded so that their
        # score doesn't inflate top_score and suppress lower-scoring but genuinely
        # relevant tests (e.g. sparse-file expansion results at high-rc regions).
        # When there are no direct hits at all (pr changes code with zero CIDB coverage),
        # top_score stays 0.0 and effective_min defaults to MIN_SCORE so ALL tests survive.
        top_score = 0.0
        for quads in line_to_tests.values():
            for t, width, depth, region_test_count, pass_weight in quads:
                if pass_weight >= self.PASS_WEIGHT_DIRECT:
                    rc = max(1, region_test_count)
                    s = pass_weight / (width * rc)
                    if s > top_score:
                        top_score = s
        effective_min = min(MAX_EFFECTIVE_MIN, max(MIN_SCORE, top_score / MAX_SCORE_RATIO))
        all_ranked = sorted(width_score, key=sort_key)
        ranked = [t for t in all_ranked if width_score[t] >= effective_min][:MAX_OUTPUT_TESTS]

        # Broad-tier2 guarantee: ensure the top-N high-cov_regions broad-tier2 tests
        # are always in the output, even if the score-ranked list is already at the cap.
        #
        # Problem without this: when ALL evidence is from broad-tier2 regions (e.g.
        # HTTPConnectionPool.cpp where the changed lines only have rc=5918 coverage),
        # 5918+ tests all get nearly equal scores.  The cap cuts them to MAX_OUTPUT_TESTS
        # in an effectively random order, potentially omitting the most relevant tests
        # (those with highest cov_regions, covering more of the changed hunks).
        #
        # Fix: after ranking, replace the last guarantee.size tests in the ranked list
        # with the top-cov_regions tests from the guarantee that aren't already present.
        # This ensures the most-covering broad-tier2 tests are always included.
        # Broad-tier2 guarantee: if the cap cut off high-cov_regions broad-tier2 tests,
        # append the top few (by cov_regions) that didn't make it — but only up to the cap.
        broad_guarantee = getattr(self, '_broad_tier2_guarantee', [])
        if broad_guarantee and len(ranked) < MAX_OUTPUT_TESTS:
            ranked_set = set(ranked)
            slots = MAX_OUTPUT_TESTS - len(ranked)
            extra = [t for t in broad_guarantee if t not in ranked_set][:slots]
            if extra:
                ranked = ranked + extra
                print(f"[find_tests] broad-tier2 guarantee: +{len(extra)} high-cov_regions tests added")

        # Keyword guarantee: always include top keyword-matched tests even when their
        # score is too low to survive the 300-cap against many direct hits.
        # Replace the last N tail items with keyword tests not already present.
        # This ensures domain-specific tests (e.g. 03444_analyzer_resolve_alias_columns
        # for QueryAnalyzer.cpp changes) are never silently dropped.
        keyword_guarantee = getattr(self, '_keyword_guarantee', [])
        if keyword_guarantee:
            ranked_set = set(ranked)
            kw_extra = [t for t in keyword_guarantee if t not in ranked_set]
            if kw_extra:
                # Replace tail items to stay within MAX_OUTPUT_TESTS
                n = min(len(kw_extra), MAX_OUTPUT_TESTS)
                ranked = ranked[: MAX_OUTPUT_TESTS - n] + kw_extra[:n]
                print(f"[find_tests] keyword guarantee: +{len(kw_extra[:n])} domain tests injected")

        narrow_count = sum(1 for t in ranked if has_narrow_hit[t])
        direct_count = sum(
            1 for t in ranked
            if has_narrow_hit[t] and min_depth_seen.get(t, 255) <= self.DIRECT_CALL_MAX_DEPTH
        )
        broad_count = len(ranked) - narrow_count

        info = "Tests found for lines:\n"
        if not line_to_tests:
            info += "  No changed lines found in diff\n"
        else:
            for (file_, line_), pairs in line_to_tests.items():
                if pairs:
                    info += f"  {file_}:{line_} -> {len(pairs)} tests\n"
        scored_total = len(width_score)
        filtered_out = scored_total - len(ranked)
        info += (
            f"Total unique tests: {len(ranked)} "
            f"({direct_count} direct-narrow, {narrow_count - direct_count} indirect-narrow, "
            f"{broad_count} broad"
        )
        if filtered_out > 0:
            info += f"; {filtered_out} below score threshold"
        info += ")\n"
        if ranked:
            top = ranked[0]
            d = min_depth_seen.get(top, 255)
            tier = ("direct-narrow" if has_narrow_hit[top] and d <= self.DIRECT_CALL_MAX_DEPTH
                    else "narrow" if has_narrow_hit[top] else "broad")
            info += f"Top test: {top} (score={width_score[top]:.6f}, {tier}, depth={d})\n"
            bottom = ranked[-1]
            info += f"Bottom test: {bottom} (score={width_score[bottom]:.6f})\n"

        return ranked, Result(
            name="tests found by coverage", status=Result.StatusExtended.OK, info=info
        )

    def get_all_relevant_tests_with_info(self):
        # Use a list to preserve insertion order and a seen set to deduplicate.
        # Priority: changed/new tests first, then previously failed, then
        # coverage-ranked tests (most changed lines covered first).
        seen: set = set()
        ranked: list = []
        results = []

        def add_tests(new_tests):
            for t in new_tests:
                if t not in seen:
                    seen.add(t)
                    ranked.append(t)

        # Integration tests run changed test suboptimally (entire module), it might be too long
        # limit it to stateless tests only
        if self.job_type == self.STATELESS_JOB_TYPE:
            changed_tests, result = self.get_changed_or_new_tests_with_info()
            add_tests(changed_tests)
            results.append(result)

        previously_failed_tests, result = self.get_previously_failed_tests_with_info()
        add_tests(previously_failed_tests)
        results.append(result)

        # TODO: Add coverage support for Integration tests
        if self.job_type == self.STATELESS_JOB_TYPE:
            try:
                covering_tests, result = self.get_most_relevant_tests()
                add_tests(covering_tests)
                results.append(result)
            except Exception as e:
                print(
                    f"WARNING: Failed to get coverage-based tests (best effort): {e}",
                    file=sys.stderr,
                )
                results.append(
                    Result(
                        name="tests found by coverage",
                        status=Result.StatusExtended.OK,
                        info=f"Skipped: {e}",
                    )
                )
                raise

        return ranked, Result(
            name="Fetch relevant tests",
            status=Result.Status.SUCCESS,
            info=f"Found {len(ranked)} relevant tests",
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
        import types
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
