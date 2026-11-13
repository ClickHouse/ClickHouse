"""Choose whole performance tests using query coverage and the shared selectors."""

import difflib
import hashlib
import json
import re
import subprocess
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from ci.jobs.performance_coverage_sharding import test_identity
from ci.jobs.scripts.coverage_selection import (
    attach_bracket_owners,
    build_bracket_owners_queries,
    build_bracket_spans_queries,
    build_candidate_queries,
    canonical_coverage_paths,
    find_brackets,
    parse_rows,
    protect_selection,
    rank_candidates,
    snapshot_predicate,
    sql_string,
    timestamp,
    validate_snapshots,
)
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.perf import test_discovery
from ci.jobs.scripts.test_selection_config import (
    INTEGRATION_SELECTION_CONFIG,
    SELECTION_CONFIG,
)

VERSION = "performance-combined-query300-header300-fixed196-v1"
FIXED_TESTS = Path(__file__).with_name("performance_fixed_tests.json")
COVERAGE_CHECK = "Performance Comparison%per_query_coverage%"
QUERY_ID = r"^(.+)\.query([0-9]+)\.([0-9a-f]+)\.(first|warm)$"
HEADER_SUFFIXES = (".h", ".hpp", ".hh", ".hxx", ".inc")
# Selection costs two GitHub API requests per changed file in each of the shards; above this the run uses the full suite.
MAX_SELECTION_CHANGED_FILES = 200
# The compiler images; the other build inputs come from `filter_job.py`.
BUILD_PATHS = ("ci/docker/cctools/", "ci/docker/fasttest/", "ci/docker/binary-builder/")
# The runner, fixtures and server configuration of every test: no covered source line changes.
HARNESS_PATHS = (
    "tests/performance/scripts/",
    "tests/performance/user_files/",
    "tests/config/top_level_domains/",
    "tests/config/config.d/top_level_domains_lists.xml",
    "ci/jobs/scripts/perf/",
    "ci/jobs/performance_tests.py",
    "ci/jobs/scripts/dataset_download.py",
    "ci/jobs/scripts/seaweedfs_service.py",
    "ci/jobs/scripts/functional_tests/setup_seaweedfs.sh",
    "ci/docker/test-base/",
    "ci/docker/performance-comparison/",
    "programs/server/config.xml",
    "programs/server/users.xml",
    "programs/server/config.d/",
)


def full_suite_reason(info, build_type):
    from ci.jobs.scripts.workflow_hooks.filter_job import (
        _has_arch_sensitive_changes,
        _has_submodule_changes,
        _has_uncounted_build_changes,
    )

    if info.pr_number <= 0:
        return "Not a pull request"
    if "ci-force-all" in info.pr_labels:
        return "Requested by ci-force-all"
    # Missing inputs fail in `select`, not in the pre-hook that starts the whole workflow.
    changed = [f.removeprefix("./") for f in info.get_changed_files() or []]
    if len(changed) > MAX_SELECTION_CHANGED_FILES:
        return f"Too many changed files for test selection: {len(changed)} > {MAX_SELECTION_CHANGED_FILES}"
    # Labels and automatic full-suite requests use ARM; AMD keeps query coverage selection.
    if build_type != "arm_release":
        return ""
    if "pr-performance" in info.pr_labels:
        return "Requested by pr-performance"
    if (
        _has_submodule_changes(changed)
        or _has_uncounted_build_changes(changed)
        or _has_arch_sensitive_changes(changed)
        or any(
            f.startswith(BUILD_PATHS) or f.endswith((".cmake", "CMakeLists.txt"))
            for f in changed
        )
    ):
        return (
            "Build change: submodules, build files and compiler images have no coverage"
        )
    if any(f.startswith(HARNESS_PATHS) for f in changed):
        return "Performance harness, fixtures or server configuration changed"
    # Settings can keep the same default expression while a constant it uses changes.
    # These definitions have no executable coverage regions of their own.
    if "src/Core/Defines.h" in changed:
        return "Setting default definitions changed"
    # `src/Core/Settings.cpp` is excluded from coverage: every test reads settings.
    settings = info.get_kv_data("settings_declaration_changes") or {"declarations": []}
    if any(d["default_changed"] for d in settings["declarations"]):
        return "Setting default changed"
    return ""


def pr_shards(info, build_type):
    return 8 if full_suite_reason(info, build_type) else 6


def performance_config(base):
    # Only suite identities, health thresholds and the approved performance
    # parameters change. Ranking, bracketing and protection are shared.
    return replace(
        base,
        max_precise_region_owners=300,
        coverage_check_name_like=COVERAGE_CHECK,
        coverage_test_name_pattern=r"^[A-Za-z0-9_]",
        coverage_shards=16,
        coverage_run_count=1,
        min_exported_tests_per_shard=1,
    )


def query_coverage_sql(query):
    # Count whole XML owners, not query/phase identities. Setup and teardown
    # never contribute to selection, but the runner still executes them.
    source = f"""(
        SELECT check_start_time, check_name, file, line_start, line_end,
               min_depth, branch_flag,
               extract(raw.test_name, {sql_string(QUERY_ID)}) AS test_name
        FROM checks_coverage_lines AS raw
        WHERE match(raw.test_name, {sql_string(QUERY_ID)})
    )"""
    return query.replace("FROM checks_coverage_lines", "FROM " + source)


def previous_failures_query(pr_number, cutoff):
    # Performance exports query identities, not the bare test names and FAIL
    # statuses used by the stateless selector. Execution errors have their own
    # table. Keep the same 30-day window and widespread-failure exclusion, with
    # whole XML owners as the unit counted by that exclusion.
    cutoff = sql_string(cutoff)
    identity = sql_string(r"^([A-Za-z0-9_-]+)(?: #[0-9]+(?:::(?:new|old))?)?$")
    return f"""
        WITH failures AS (
            SELECT commit_sha, check_name, check_start_time,
                   extract(test_name, {identity}) AS owner
            FROM checks
            WHERE pull_request_number = {int(pr_number)}
              AND check_name LIKE 'Performance Comparison%master_head%'
              AND check_name NOT LIKE '%iceberg%'
              AND check_status = 'failure' AND test_status IN ('slower', 'FAIL')
              AND check_start_time >= toDateTime({cutoff}, 'UTC') - INTERVAL 30 DAY
              AND check_start_time < toDateTime({cutoff}, 'UTC')
            UNION ALL
            SELECT new_sha AS commit_sha,
                   decodeURLComponent(extractURLParameter(report_url, 'name_1')) AS check_name,
                   check_start_time, test AS owner
            FROM perf_run_errors_v1
            WHERE pr_number = {int(pr_number)} AND baseline_kind = 'master_head'
              AND arch IN ('amd', 'arm') AND match(test, '^[A-Za-z0-9_-]+$')
              AND check_start_time >= toDateTime({cutoff}, 'UTC') - INTERVAL 30 DAY
              AND check_start_time < toDateTime({cutoff}, 'UTC')
        ), isolated_failures AS (
            SELECT commit_sha, check_name, max(check_start_time) AS last_failure,
                   groupUniqArray(owner) AS tests
            FROM failures WHERE owner != '' AND check_name != ''
            GROUP BY commit_sha, check_name HAVING length(tests) < 20
        )
        SELECT arrayJoin(tests) AS test_name FROM isolated_failures
        GROUP BY test_name
        ORDER BY count() * exp(-dateDiff('day', max(last_failure),
            toDateTime({cutoff}, 'UTC')) / 7.) DESC
        FORMAT JSONEachRow
    """


def map_lines(source, target):
    """Translate only identical, uniquely anchored blocks into binary coordinates."""
    if source == target:
        return {n: n for n in range(1, len(source) + 1)}
    result = {}
    old_text, new_text = (
        "\n" + "\n".join(source) + "\n",
        "\n" + "\n".join(target) + "\n",
    )
    for a, b, count in difflib.SequenceMatcher(
        None, source, target, autojunk=False
    ).get_matching_blocks():
        if not count:
            continue
        block = "\n" + "\n".join(source[a : a + count]) + "\n"
        if old_text.count(block) == new_text.count(block) == 1:
            result.update((a + n + 1, b + n + 1) for n in range(count))
    return result


def translate_diff(diff, read_source):
    changed = Targeting._parse_diff_lines(diff)
    hunks = Targeting._parse_diff_hunk_ranges(diff)
    by_file = defaultdict(set)
    for path, line in changed:
        if (
            path.startswith(Targeting.COVERAGE_SOURCE_PREFIXES)
            and path not in Targeting.SHARED_REGISTRY_FILES
        ):
            by_file[path].add(line)
    points, ranges, diagnostics = [], {}, []
    for path, lines in sorted(by_file.items()):
        before, covered = read_source(path)
        mapping = map_lines(before, covered)
        mapped = {(path, mapping[line]) for line in lines if line in mapping}
        points.extend(mapped)
        spans = []
        for lo, hi in hunks[path]:
            last_source = None
            for line in range(lo, hi + 1):
                if line not in mapping:
                    last_source = None
                    continue
                dest = mapping[line]
                if spans and last_source == line - 1 and spans[-1][1] == dest - 1:
                    spans[-1][1] = dest
                else:
                    spans.append([dest, dest])
                last_source = line
        if mapped:
            ranges[path] = [tuple(span) for span in spans]
        diagnostics.append(
            {
                "file": path,
                "mapped_points": len(mapped),
                "unmapped_points": sorted(lines - mapping.keys()),
            }
        )
    return sorted(points), ranges, diagnostics


def manifest_url(job):
    # The collector's public per-revision layout; a same-SHA retry replaces it.
    branch = urllib.parse.quote(job["head_ref"], safe="/")
    name = re.sub(r"[^a-z0-9]+", "_", job["check_name"].lower()).strip("_")
    return f"https://s3.amazonaws.com/clickhouse-test-reports/REFs/{branch}/{job['commit_sha']}/nightlycoverage/{name}/manifest.json"


def coverage_identity(manifest):
    # A corrected wrapper can finish a collection without changing its inputs.
    # The workflow SHA binds each manifest to its own successful job below;
    # compatibility depends on the executable, source inputs and assignment.
    return (
        manifest["binary_sha"],
        manifest["binary_sha256"],
        manifest["source_identity"],
        manifest["warm_runs"],
        manifest["resume_protocol"],
        manifest["dataset_layout"],
        manifest["sharding"]["assignment_sha256"],
    )


def coverage_cohort(jobs, read_manifest):
    """Take the freshest complete set of compatible shards, including retries."""
    groups, seen_urls, superseded = defaultdict(dict), set(), []
    for job in sorted(
        jobs, key=lambda row: timestamp(row["check_start_time"]), reverse=True
    ):
        url = manifest_url(job)
        if url in seen_urls:
            continue
        seen_urls.add(url)
        manifest = read_manifest(url)
        if (
            manifest["sha"] != job["commit_sha"]
            or manifest["check_name"] != job["check_name"]
        ):
            raise ValueError("Manifest does not belong to the successful coverage job")
        if manifest["check_start_time"] != job["check_start_time"]:
            # Re-running a job at the same SHA replaces its artifact. Never bind
            # the new manifest to an older successful publication in CIDB.
            superseded.append(job)
            continue
        if not manifest["complete"]:
            raise ValueError("Successful coverage job has an incomplete manifest")
        plan = manifest["sharding"]
        shard = re.search(r", (\d+)/(\d+)\)$", job["check_name"])
        if (
            not shard
            or int(shard[2]) != 16
            or plan["shards"] != 16
            or plan["shard"] != int(shard[1])
            or not 1 <= plan["shard"] <= 16
        ):
            raise ValueError("Coverage manifest has an unexpected shard assignment")
        group = groups[coverage_identity(manifest)]
        group.setdefault(plan["shard"], (job, manifest))
        if len(group) == 16:
            # Descending start time minimizes the age of the oldest shard. A
            # partial newer collection cannot displace a complete older one.
            selected = [group[n] for n in range(1, 17)]
            return [j for j, _ in selected], [m for _, m in selected], superseded
    raise ValueError("No complete compatible performance coverage within 72 hours")


def validate_manifests(manifests, cutoff):
    if len(manifests) != 16:
        raise ValueError(
            "Performance selection requires a complete 16-shard collection"
        )
    identities, snapshots, tests, assignment = set(), [], {}, {}
    for manifest in manifests:
        if not manifest["complete"] or manifest["warm_runs"] != 1:
            raise ValueError("Incomplete coverage or changed repetition policy")
        plan = manifest["sharding"]
        identities.add(coverage_identity(manifest))
        collected = 0
        for name, state in manifest["tests"].items():
            if name in assignment:
                raise ValueError(f"Duplicate coverage test: {name}")
            assignment[name] = plan["shard"]
            if state["status"] == "skipped":
                continue
            if state["status"] != "collected":
                raise ValueError(f"Unfinished coverage test: {name}")
            tests[name] = plan["timings"][name]
            collected += 1
        snapshots.append(
            {
                "check_start_time": manifest["check_start_time"],
                "check_name": manifest["check_name"],
                "exported_tests": collected,
            }
        )
    if len(identities) != 1:
        raise ValueError(
            "Coverage shards have incompatible binary, source or assignment identities"
        )
    if (
        hashlib.sha256(json.dumps(assignment, sort_keys=True).encode()).hexdigest()
        != manifests[0]["sharding"]["assignment_sha256"]
    ):
        raise ValueError("Coverage test universe differs from its assignment hash")
    validate_snapshots(snapshots, cutoff, performance_config(SELECTION_CONFIG))
    return snapshots, tests


def validate_phases(rows, manifests):
    """Confirm CIDB has both query phases and test-wide setup/teardown."""
    actual = defaultdict(dict)
    for row in rows:
        key = row["check_start_time"], row["check_name"]
        if int(row["rows"]) <= 0 or int(row.get("invalid_rows", 0)):
            raise ValueError("Empty coverage phase")
        if row["test_name"] in actual[key]:
            raise ValueError("Duplicate coverage phase")
        actual[key][row["test_name"]] = row
    for manifest in manifests:
        if manifest.get("phase_manifest_version") not in (None, 1):
            raise ValueError("Unsupported performance phase manifest version")
        key = manifest["check_start_time"], manifest["check_name"]
        names = actual.pop(key, {})
        expected_tests = {
            Path(name).stem
            for name, state in manifest["tests"].items()
            if state["status"] == "collected"
        }
        queries = defaultdict(lambda: defaultdict(set))
        test_phases = defaultdict(set)
        phase_names_by_test = defaultdict(list)
        for identity in names:
            match = re.fullmatch(QUERY_ID, identity)
            if match:
                test, index, digest, phase = match.groups()
                queries[test][(int(index), digest)].add(phase)
            elif identity.endswith((".setup", ".teardown")):
                test, phase = identity.rsplit(".", 1)
                test_phases[test].add(phase)
            else:
                raise ValueError(f"Unknown coverage identity: {identity}")
            phase_names_by_test[test].append(identity)
        expected_query_tests = {
            test
            for test in expected_tests
            if (
                manifest["tests"][test + ".xml"]["query_count"] > 0
                if manifest.get("phase_manifest_version") == 1
                else test in queries
            )
        }
        if set(queries) != expected_query_tests or set(test_phases) != expected_tests:
            raise ValueError("CIDB test universe differs from the completed manifests")
        for test in expected_tests:
            phases = queries[test]
            if test_phases[test] != {"setup", "teardown"} or any(
                p != {"first", "warm"} for p in phases.values()
            ):
                raise ValueError(f"Missing coverage phases for {test}")
            state = manifest["tests"][test + ".xml"]
            if manifest.get("phase_manifest_version") == 1:
                phase_names = sorted(phase_names_by_test[test])
                if (
                    len(phases) != state["query_count"]
                    or hashlib.sha256("\n".join(phase_names).encode()).hexdigest()
                    != state["phase_names_sha256"]
                ):
                    raise ValueError(
                        f"CIDB publication differs from collected phases for {test}"
                    )
            elif sorted(index for index, _ in phases) != list(range(len(phases))):
                raise ValueError(
                    f"Missing or conflicting coverage query indices for {test}"
                )
    if actual:
        raise ValueError("CIDB returned unexpected snapshots")


class PerformanceSelection(Targeting):
    def __init__(self, info, query=None):
        super().__init__(info)
        self.config = performance_config(SELECTION_CONFIG)
        self.query = query or (
            lambda sql: self._ci_db().query(sql, log_level="", timeout=180)
        )
        self._sources = {}

    def rows(self, sql, coverage=False):
        return parse_rows(self.query(query_coverage_sql(sql) if coverage else sql))

    def snapshot(self):
        cutoff = self.selection_cutoff()
        jobs = self.rows(f"""
            SELECT DISTINCT commit_sha, head_ref, check_name, check_start_time, task_url
            FROM checks
            WHERE workflow_name = 'NightlyCoverage' AND test_name = ''
              AND check_name LIKE {sql_string(COVERAGE_CHECK)} AND check_status = 'success'
              AND check_start_time > toDateTime({sql_string(cutoff)}, 'UTC') - INTERVAL 72 HOUR
              AND check_start_time + toIntervalSecond(ceil(check_duration_ms / 1000)) < toDateTime({sql_string(cutoff)}, 'UTC')
            ORDER BY check_start_time DESC FORMAT JSONEachRow
        """)

        def read_manifest(url):
            with urllib.request.urlopen(url, timeout=60) as response:
                return json.load(response)

        jobs, manifests, superseded = coverage_cohort(jobs, read_manifest)
        snapshots, tests = validate_manifests(manifests, cutoff)
        rows = self.rows(f"""
            SELECT check_start_time, check_name, test_name, count() AS rows,
                   countIf(empty(file) OR line_start = 0 OR line_start > line_end OR branch_flag NOT IN (0, 1, 2)) AS invalid_rows
            FROM checks_coverage_lines WHERE {snapshot_predicate(snapshots)}
            GROUP BY check_start_time, check_name, test_name FORMAT JSONEachRow
        """)
        validate_phases(rows, manifests)
        self.selection_diagnostics["coverage"] = {
            "jobs": jobs,
            "superseded_artifacts": superseded,
            "snapshots": snapshots,
            "binary_sha": manifests[0]["binary_sha"],
            "binary_sha256": manifests[0]["binary_sha256"],
            "assignment_sha256": manifests[0]["sharding"]["assignment_sha256"],
            "source_identity": manifests[0]["source_identity"],
            "tests": len(tests),
            "phases": len(rows),
            "phase_identity_checksums": all(
                m.get("phase_manifest_version") == 1 for m in manifests
            ),
            "legacy_manifest_limitation": (
                "Legacy exports rely on successful producer completion for the expected query count; "
                "phase pairing and test completeness are checked here."
                if any(m.get("phase_manifest_version") != 1 for m in manifests)
                else None
            ),
        }
        return snapshots, tests, manifests[0]["binary_sha"]

    def fetch_revisions(self, *revisions):
        """The checkout is shallow: fetch the revisions `source` reads, without history."""
        subprocess.run(
            ["git", "fetch", "--depth=1", "--no-tags", "--no-recurse-submodules", "origin", *revisions],
            check=True,
        )

    def source(self, revision, path):
        key = revision, path
        if key not in self._sources:
            if subprocess.run(["git", "cat-file", "-e", f"{revision}:{path}"], stderr=subprocess.DEVNULL).returncode:
                # A file added after collection has no source coordinates in
                # that binary. It is reported as unmapped, never as coverage.
                self._sources[key] = []
            else:
                self._sources[key] = subprocess.run(
                    ["git", "show", f"{revision}:{path}"],
                    check=True,
                    stdout=subprocess.PIPE,
                    text=True,
                    errors="replace",
                ).stdout.splitlines()
        return self._sources[key]

    def select(self, test_files, directory=Path("tests/performance")):
        if self.info.get_changed_files() is None or self.info.get_kv_data(
            "settings_history_fetch_error"
        ):
            raise RuntimeError(
                "Changed files or setting default changes are unavailable"
            )
        snapshots, covered_tests, binary_sha = self.snapshot()
        metadata = self._github_get(f"pulls/{self.pr_number}").json()
        if metadata["head"]["sha"] != self.info.sha:
            raise ValueError("PR head changed before performance selection")
        compare = f"compare/{metadata['base']['sha']}...{self.info.sha}"
        base = self._github_get(compare).json()["merge_base_commit"]["sha"]
        diff = self._github_get(compare, accept="application/vnd.github.diff").text
        self.fetch_revisions(base, binary_sha)
        # Added files have no old text. They are explicitly recorded as unmapped,
        # not attributed to arbitrary nearby code or a fabricated full-suite rule.
        added = {m[1] for m in re.finditer(r"--- /dev/null\n\+\+\+ b/(.+)", diff)}

        def read_source(path):
            if path in added:
                return [], []
            return self.source(base, path), self.source(binary_sha, path)

        points, hunks, mapping = translate_diff(diff, read_source)
        available = {
            Path(name).stem: name
            for name in test_files
            if not test_discovery.test_is_long(directory / name)
        }
        changed = sorted(
            name
            for name, xml in available.items()
            if xml not in covered_tests
            or test_identity(directory / xml) != covered_tests[xml]["input_sha256"]
        )
        failed = sorted(
            {
                row["test_name"]
                for row in self.rows(
                    previous_failures_query(self.pr_number, self.selection_cutoff())
                )
            }
            & available.keys()
        )
        policies = {}
        for name, base_config in (
            ("stateless", SELECTION_CONFIG),
            ("integration", INTEGRATION_SELECTION_CONFIG),
        ):
            config = performance_config(base_config)
            regions, brackets = [], []
            if points:
                for sql in build_candidate_queries(points, hunks, snapshots, config):
                    regions.extend(self.rows(sql, coverage=True))
                if config.bracket_gap_lines:
                    spans = [
                        row
                        for sql in build_bracket_spans_queries(hunks, snapshots, config)
                        for row in self.rows(sql, coverage=True)
                    ]
                    brackets = find_brackets(hunks, spans, config)
                    if brackets:
                        owners = [
                            row
                            for sql in build_bracket_owners_queries(
                                brackets, snapshots, config
                            )
                            for row in self.rows(sql, coverage=True)
                        ]
                        attach_bracket_owners(brackets, owners)
            ranked = rank_candidates(
                regions, points, hunks, snapshots, config, brackets
            )
            candidates = [row for row in ranked if row["test"] in available]
            policies[name] = protect_selection(
                changed if name == "stateless" else [], failed, candidates, str, config
            )
        headers = sorted(
            {
                path
                for path, _ in Targeting._parse_diff_lines(diff)
                if path.startswith(Targeting.COVERAGE_SOURCE_PREFIXES)
                and path.endswith(HEADER_SUFFIXES)
                and path not in Targeting.SHARED_REGISTRY_FILES
            }
        )
        header_details, header_tests = [], set()
        for path in headers:
            paths = ",".join(map(sql_string, canonical_coverage_paths(path)))
            rows = self.rows(
                f"""SELECT groupUniqArray(test_name) AS tests FROM checks_coverage_lines
                WHERE {snapshot_predicate(snapshots)} AND file IN ({paths}) FORMAT JSONEachRow""",
                coverage=True,
            )
            owners = set(rows[0]["tests"])
            admitted = owners & available.keys() if len(owners) <= 300 else set()
            header_tests.update(admitted)
            header_details.append(
                {
                    "file": path,
                    "covering_tests": len(owners),
                    "selected": sorted(admitted),
                }
            )
        fixed = set(json.loads(FIXED_TESTS.read_text())["tests"])
        selected = set(changed) | header_tests | (fixed & available.keys())
        for policy in policies.values():
            selected.update(row["test"] for row in policy["selected"])
        self.selection_diagnostics.update(
            {
                "version": VERSION,
                "head_sha": self.info.sha,
                "diff_base_sha": base,
                "mapping": mapping,
                "policies": policies,
                "headers": header_details,
                "fixed": sorted(fixed & available.keys()),
                "absent_fixed": sorted(fixed - available.keys()),
                "changed_or_uncovered": changed,
                "previously_failed": failed,
                "selected": sorted(selected),
                "available_count": len(available),
                "query_policy": "Normal adaptive timings; all queries in every selected XML",
            }
        )
        selected_files = sorted(available[name] for name in selected)
        return selected_files, set(selected_files)
