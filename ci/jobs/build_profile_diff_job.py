"""Build profile diff check (PR workflow).

Compares the PR's aarch64 release build profile against the latest master
build, using the CI logs cluster populated by
ci/jobs/scripts/job_hooks/build_profile_hook.py:

  * binary_sizes      - final binaries and every intermediate .o file
  * build_time_trace  - clang -ftime-trace and lld --time-trace events:
                        per-TU compile time, per-entity frontend time
                        (template instantiations, header parsing), and
                        per-function ThinLTO link time
  * binary_symbols    - per-symbol sizes of the final binaries

The PR side is uploaded by the post-hook of the `Build (arm_release)` job
(reduced time trace, see LogClusterBuildProfileQueries.REDUCED_PROFILE_EVENTS);
the baseline is the most recent master commit with uploaded data.

If there is a significant change the check posts a PR comment (kept updated in
place on repeated runs) with the details; otherwise the comment states that
there are no significant changes.

Notes on data coverage:
  * PR and master builds use sccache, so `build_time_trace` contains compile
    events only for translation units that were actually recompiled. Per-TU
    compile times are therefore compared against the most recent master build
    that recompiled the same TU.
  * The ThinLTO link runs on every build, so link time and per-function
    OptFunction events are always present on both sides.
  * `binary_sizes` and `binary_symbols` are complete on both sides.

Local run (bypasses AWS SSM secrets and GitHub):
  CI_LOGS_HOST=... CI_LOGS_PASSWORD=... CI_LOGS_USER=default \\
  python3 -m ci.jobs.build_profile_diff_job \\
      --local --pr-sha <sha> --pr-number <n> [--base-sha <sha>]
"""

import argparse
import dataclasses
import json
import os
import subprocess
import traceback
from typing import List, Optional

from ci.jobs.scripts.log_cluster import LogCluster
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

CHECK_NAME = "arm_release"
COMMENT_TAG = "build-profile-diff"

BUILD_DIR = "./ci/tmp/build"
MAIN_BINARY = f"{BUILD_DIR}/programs/clickhouse"
HEADLINE_BINARIES = [
    f"{BUILD_DIR}/programs/clickhouse-stripped",
    f"{BUILD_DIR}/programs/clickhouse",
    f"{BUILD_DIR}/programs/self-extracting/clickhouse",
    f"{BUILD_DIR}/programs/clickhouse-keeper",
]
LINK_BINARIES = [
    f"{BUILD_DIR}/programs/clickhouse",
    f"{BUILD_DIR}/programs/clickhouse-keeper",
]

# How far back to look for data. The baseline commit is at most hours old; the
# per-TU compile baseline needs a longer window because a TU enters the trace
# only when a master build actually recompiles it (sccache miss).
PR_DAYS = 7
BASE_DAYS = 7
TU_BASE_DAYS = 14

# Significance thresholds. Measured noise between adjacent master arm_release
# builds: final binary sizes differ by O(100) bytes (objects are byte-identical
# sccache hits, ThinLTO relink wiggles the rest), so size thresholds are tight.
# Times run on different machines under different load and need generous
# margins.
BINARY_SIG_BYTES = 1 << 20  # final binary: 1 MiB or
BINARY_SIG_RATIO = 0.002  # 0.2%
OBJECT_REPORT_BYTES = 16 << 10  # .o file: report at 16 KiB,
OBJECT_SIG_BYTES = 256 << 10  # significant at 256 KiB
LINK_SIG_RATIO = 0.15  # link wall time: 15% and
LINK_SIG_SECONDS = 120  # 2 minutes
OPTFN_REPORT_SECONDS = 2  # per-function LTO time: report at |delta| >= 2s
OPTFN_REPORT_RATIO = 1.5  # and 1.5x, significant at 15s
OPTFN_SIG_SECONDS = 15
TU_REPORT_SECONDS = 5  # per-TU compile time: report at |delta| >= 5s
TU_REPORT_RATIO = 1.3  # and 1.3x, significant at 20s and 1.5x
TU_SIG_SECONDS = 20
TU_SIG_RATIO = 1.5
SYMBOL_REPORT_BYTES = 16 << 10  # per-symbol size: report at 16 KiB,
SYMBOL_SIG_BYTES = 256 << 10  # significant at 256 KiB
MAX_TABLE_ROWS = 20
MAX_NAME_LEN = 100


@dataclasses.dataclass
class Section:
    """One comparison aspect: a markdown fragment plus its verdict."""

    title: str
    body: str = ""  # markdown, empty = nothing to show
    significant: bool = False
    summary: str = ""  # one line for the job result info


class Db:
    def __init__(self):
        # Local runs read the connection from the environment instead of AWS
        # SSM (any HTTPS ClickHouse endpoint with the CI logs schema works).
        url = os.environ.get("CI_LOGS_HOST", "")
        if url:
            if not url.startswith("http"):
                url = f"https://{url}:8443"
            password = os.environ.get("CI_LOGS_PASSWORD", os.environ.get("CI_LOGS_PASWORD", ""))
            self._cluster = LogCluster(
                url=url,
                user=os.environ.get("CI_LOGS_USER", "default"),
                password=password,
            )
        else:
            self._cluster = LogCluster()

    def query(self, query: str) -> List[dict]:
        """Run a SELECT and return rows as dicts. Raises on failure."""
        response = self._cluster.select(query + " FORMAT JSON")
        if response is None:
            raise RuntimeError(f"CI logs cluster query failed: {query}")
        return json.loads(response)["data"]


def quote(s: str) -> str:
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def in_list(values) -> str:
    return ", ".join(quote(v) for v in values)


@dataclasses.dataclass
class Side:
    """One comparison side, pinned to a single concrete build run.

    A rerun of the build re-inserts the whole dataset under the same
    pull_request_number/commit_sha/check_name with a fresh check_start_time and
    instance_id, and the three tables of one build are uploaded under the same
    pair (see build_profile_hook.py). The pair is chosen once per side (see
    resolve_run) and reused across every table, so the whole comparison stays
    scoped to one concrete build instead of picking a different run per table -
    which would let a size section read the newest run while the symbol section
    silently falls back to an older one.
    """

    days: int
    pr_number: int
    sha: str
    check_start_time: str
    instance_id: str


def resolve_run(db: "Db", days: int, pr_number: int, sha: str, table: str = "binary_sizes") -> Optional[Side]:
    """The newest uploaded build run for one side, or None if there is no data.

    Resolved from `binary_sizes` by default (the canonical, always-complete
    table); the resulting (check_start_time, instance_id) is reused for every
    other table of the same build.
    """
    rows = db.query(
        f"""SELECT check_start_time, instance_id
        FROM {table}
        WHERE date >= today() - {days}
            AND pull_request_number = {pr_number}
            AND commit_sha = {quote(sha)}
            AND check_name = {quote(CHECK_NAME)}
        ORDER BY check_start_time DESC
        LIMIT 1"""
    )
    if not rows:
        return None
    return Side(days, pr_number, sha, rows[0]["check_start_time"], rows[0]["instance_id"])


def side_conditions(side: Side, extra_where: str = "") -> str:
    """The WHERE conditions selecting one side's rows, pinned to its build run."""
    where = f"\n            AND {extra_where}" if extra_where else ""
    return f"""date >= today() - {side.days}
            AND pull_request_number = {side.pr_number}
            AND commit_sha = {quote(side.sha)}
            AND check_name = {quote(CHECK_NAME)}
            AND check_start_time = {quote(side.check_start_time)}
            AND instance_id = {quote(side.instance_id)}{where}"""


def both_sides(
    table: str,
    columns: str,
    pr_side: Side,
    base_side: Side,
    extra_where: str = "",
) -> str:
    """A UNION ALL subquery with PR rows marked side='pr' and master rows side='base'.

    Each side is pinned to the single build run chosen for it (see Side).
    """
    pr_cond = side_conditions(pr_side, extra_where)
    base_cond = side_conditions(base_side, extra_where)
    return f"""(
        SELECT 'pr' AS side, {columns}
        FROM {table}
        WHERE {pr_cond}
        UNION ALL
        SELECT 'base' AS side, {columns}
        FROM {table}
        WHERE {base_cond}
    )"""


def format_bytes(n: float) -> str:
    n = int(n)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024 or unit == "GiB":
            return f"{n:.2f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n} B"


def format_bytes_delta(delta: float, base: float) -> str:
    sign = "+" if delta >= 0 else "-"
    percent = f" ({sign}{abs(delta) / base * 100:.2f}%)" if base else ""
    return f"{sign}{format_bytes(abs(delta))}{percent}"


def format_seconds_delta(delta_s: float, base_s: float) -> str:
    sign = "+" if delta_s >= 0 else "-"
    percent = f" ({sign}{abs(delta_s) / base_s * 100:.0f}%)" if base_s else ""
    return f"{sign}{abs(delta_s):.1f} s{percent}"


def md_code(name: str) -> str:
    """Symbol/file name as an inline code table cell."""
    name = name.replace("|", "\\|")
    if len(name) > MAX_NAME_LEN:
        name = name[: MAX_NAME_LEN - 1] + "…"
    return f"`{name}`"


def strip_build_dir(path: str) -> str:
    return path.removeprefix(f"{BUILD_DIR}/")


_DEMANGLER = None


def demangle(name: str) -> str:
    """Demangle with c++filt when available; otherwise return as is."""
    global _DEMANGLER
    if _DEMANGLER is None:
        for tool in ("llvm-cxxfilt", "c++filt"):
            try:
                subprocess.run([tool, "--version"], capture_output=True, check=True)
                _DEMANGLER = tool
                break
            except Exception:
                continue
        if _DEMANGLER is None:
            _DEMANGLER = ""
    if not _DEMANGLER or not name.startswith("_Z"):
        return name
    try:
        out = subprocess.run([_DEMANGLER], input=name, capture_output=True, text=True, timeout=10).stdout.strip()
        return out or name
    except Exception:
        return name


class LocalInfo:
    """A minimal Info substitute for running the check outside CI."""

    repo_name = "ClickHouse/ClickHouse"
    pr_number = 0
    sha = ""

    def get_kv_data(self, key):
        return None

    def get_job_report_url(self, latest=False):
        return ""


def get_master_shas(info) -> List[str]:
    """The PR's master parent chain, newest first.

    `master_track_commits_sha` (populated by the `store_data` workflow hook) is
    the first-parent history of the master commit this PR is built on top of -
    the exact baseline set, anchored to the PR's merge base rather than to the
    global master tip. `binary_sizes`/`build_time_trace` rows with
    pull_request_number = 0 come from master AND release branches, so every
    baseline lookup must intersect with this history.

    `find_baseline` only ever considers the first 100 of these, and the hook
    already stores ~100 commits, so there is no need to page more from the API.
    """
    seen = set()
    shas = []
    # `master_track_commits_sha` is anchored to the PR's master parent; fall back
    # to `master_commits` (the global tip) only if the anchored chain is absent.
    for key in ("master_track_commits_sha", "master_commits"):
        for sha in list(info.get_kv_data(key) or []):
            if sha not in seen:
                seen.add(sha)
                shas.append(sha)
        if shas:
            break
    return shas


def find_baseline(db: Db, master_shas: List[str], pr_sha: str) -> Optional[str]:
    """The most recent master commit with uploaded arm_release profile data.

    The PR-side commit is excluded so that a re-run on an already-merged
    commit does not compare it against itself.
    """
    candidates = [sha for sha in master_shas if sha != pr_sha][:100]
    if not candidates:
        return None
    rows = db.query(
        f"""SELECT DISTINCT commit_sha
        FROM binary_sizes
        WHERE date >= today() - {BASE_DAYS}
            AND pull_request_number = 0
            AND check_name = {quote(CHECK_NAME)}
            AND file = {quote(MAIN_BINARY)}
            AND commit_sha IN ({in_list(candidates)})"""
    )
    with_data = {row["commit_sha"] for row in rows}
    for sha in candidates:
        if sha in with_data:
            return sha
    return None


def has_pr_data(db: Db, pr_number: int, pr_sha: str) -> bool:
    rows = db.query(
        f"""SELECT count() AS c
        FROM binary_sizes
        WHERE date >= today() - {PR_DAYS}
            AND pull_request_number = {pr_number}
            AND commit_sha = {quote(pr_sha)}
            AND check_name = {quote(CHECK_NAME)}
            AND file = {quote(MAIN_BINARY)}"""
    )
    return rows and int(rows[0]["c"]) > 0


def compare_binaries(db: Db, pr_side, base_side) -> Section:
    section = Section(title="Binary sizes")
    rows = db.query(
        f"""SELECT file,
            maxIf(size, side = 'pr') AS pr_size,
            maxIf(size, side = 'base') AS base_size
        FROM {both_sides("binary_sizes", "file, size", pr_side, base_side, f"file IN ({in_list(HEADLINE_BINARIES)})")}
        GROUP BY file
        ORDER BY file"""
    )
    lines = [
        "| Binary | Master | PR | Δ |",
        "|---|---:|---:|---:|",
    ]
    summaries = []
    for row in rows:
        pr_size, base_size = int(row["pr_size"]), int(row["base_size"])
        if not pr_size or not base_size:
            continue
        delta = pr_size - base_size
        name = strip_build_dir(row["file"])
        if abs(delta) >= BINARY_SIG_BYTES or abs(delta) >= base_size * BINARY_SIG_RATIO:
            section.significant = True
            summaries.append(f"{name}: {format_bytes_delta(delta, base_size)}")
        lines.append(f"| {md_code(name)} | {format_bytes(base_size)} | {format_bytes(pr_size)} | {format_bytes_delta(delta, base_size)} |")
    if len(lines) > 2:
        section.body = "\n".join(lines)
    section.summary = "; ".join(summaries)
    return section


# Object files that exist in every build under a different random path: cmake
# feature-test scratch objects (kept by --debug-trycompile) and Rust
# incremental-compilation artifacts. They are not part of the product and would
# show up as added/removed churn in every comparison.
OBJECT_FILTER = "file LIKE '%.o' AND file NOT LIKE '%CMakeScratch%' AND file NOT LIKE '%/incremental/%'"


def compare_objects(db: Db, pr_side, base_side) -> Section:
    section = Section(title="Object file sizes")
    rows = db.query(
        f"""SELECT file,
            maxIf(size, side = 'pr') AS pr_size,
            maxIf(size, side = 'base') AS base_size,
            toInt64(pr_size) - toInt64(base_size) AS delta
        FROM {both_sides("binary_sizes", "file, size", pr_side, base_side, OBJECT_FILTER)}
        GROUP BY file
        HAVING abs(delta) >= {OBJECT_REPORT_BYTES}
        ORDER BY abs(delta) DESC
        LIMIT {MAX_TABLE_ROWS}"""
    )
    totals = db.query(
        f"""SELECT
            countIf(pr_size > 0 AND base_size > 0 AND pr_size != base_size) AS changed,
            countIf(base_size = 0) AS added,
            countIf(pr_size = 0) AS removed,
            sum(toInt64(pr_size) - toInt64(base_size)) AS total_delta
        FROM (
            SELECT file,
                maxIf(size, side = 'pr') AS pr_size,
                maxIf(size, side = 'base') AS base_size
            FROM {both_sides("binary_sizes", "file, size", pr_side, base_side, OBJECT_FILTER)}
            GROUP BY file
        )"""
    )[0]
    changed, added, removed = (
        int(totals["changed"]),
        int(totals["added"]),
        int(totals["removed"]),
    )
    if not rows and not added and not removed:
        return section

    lines = [
        f"{changed} object files changed ({format_bytes_delta(int(totals['total_delta']), 0)} total), {added} added, {removed} removed.",
        "",
        "| Object file | Master | PR | Δ |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        pr_size, base_size = int(row["pr_size"]), int(row["base_size"])
        delta = int(row["delta"])
        if abs(delta) >= OBJECT_SIG_BYTES:
            section.significant = True
        base_text = format_bytes(base_size) if base_size else "new"
        pr_text = format_bytes(pr_size) if pr_size else "removed"
        lines.append(f"| {md_code(strip_build_dir(row['file']))} | {base_text} | {pr_text} | {format_bytes_delta(delta, base_size)} |")
    section.body = "\n".join(lines)
    section.summary = f"{changed} object files changed, {added} added, {removed} removed"
    return section


def compare_link_time(db: Db, pr_side, base_side) -> Section:
    section = Section(title="Link time")
    rows = db.query(
        f"""SELECT file,
            maxIf(dur, side = 'pr') AS pr_dur,
            maxIf(dur, side = 'base') AS base_dur
        FROM {both_sides("build_time_trace", "file, dur", pr_side, base_side, f"file IN ({in_list(LINK_BINARIES)}) AND name = 'ExecuteLinker'")}
        GROUP BY file
        ORDER BY file"""
    )
    lines = [
        "| Binary | Master | PR | Δ |",
        "|---|---:|---:|---:|",
    ]
    summaries = []
    for row in rows:
        pr_s, base_s = int(row["pr_dur"]) / 1e6, int(row["base_dur"]) / 1e6
        if not pr_s or not base_s:
            continue
        delta = pr_s - base_s
        name = strip_build_dir(row["file"])
        if abs(delta) >= LINK_SIG_SECONDS and abs(delta) >= base_s * LINK_SIG_RATIO:
            section.significant = True
            summaries.append(f"link {name}: {format_seconds_delta(delta, base_s)}")
        lines.append(f"| {md_code(name)} | {base_s:.0f} s | {pr_s:.0f} s | {format_seconds_delta(delta, base_s)} |")
    if len(lines) > 2:
        section.body = "\n".join(lines)
    section.summary = "; ".join(summaries)
    return section


def compare_opt_functions(db: Db, pr_side, base_side) -> Section:
    """Per-function ThinLTO optimization time of the main binary.

    This is the per-function 'how long does the backend chew on it' signal; it
    is present on every build (the link is never cached) and it is where a new
    heavyweight function or template instantiation shows up first.
    """
    section = Section(title="Slowest function optimization changes (ThinLTO)")
    report_us = int(OPTFN_REPORT_SECONDS * 1e6)
    # dur >= 50ms cuts the aggregation from ~1M rows per side to tens of
    # thousands; a function can only reach the report threshold if one side
    # exceeds it anyway, and a missing side renders as "new"/"gone".
    rows = db.query(
        f"""SELECT detail,
            sumIf(dur, side = 'pr') AS pr_dur,
            sumIf(dur, side = 'base') AS base_dur,
            toInt64(pr_dur) - toInt64(base_dur) AS delta
        FROM {both_sides("build_time_trace", "detail, dur", pr_side, base_side, f"file = {quote(MAIN_BINARY)} AND name = 'OptFunction' AND dur >= 50000")}
        GROUP BY detail
        HAVING abs(delta) >= {report_us}
            AND (pr_dur = 0 OR base_dur = 0
                 OR greatest(pr_dur, base_dur) >= least(pr_dur, base_dur) * {OPTFN_REPORT_RATIO})
        ORDER BY abs(delta) DESC
        LIMIT {MAX_TABLE_ROWS}"""
    )
    if not rows:
        return section
    lines = [
        "| Function | Master | PR | Δ |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        pr_s, base_s = int(row["pr_dur"]) / 1e6, int(row["base_dur"]) / 1e6
        delta = pr_s - base_s
        if abs(delta) >= OPTFN_SIG_SECONDS:
            section.significant = True
        base_text = f"{base_s:.1f} s" if base_s else "new"
        pr_text = f"{pr_s:.1f} s" if pr_s else "gone"
        lines.append(f"| {md_code(demangle(row['detail']))} | {base_text} | {pr_text} | {format_seconds_delta(delta, base_s)} |")
    section.body = "\n".join(lines)
    return section


def compare_compile_times(db: Db, pr_side, base_side, master_shas) -> Section:
    """Per-TU compile time of TUs this PR recompiled.

    sccache makes the recompiled set exactly the TUs affected by the PR. Each
    is compared against the most recent master build that also recompiled it.
    """
    section = Section(title="Compile time of recompiled translation units")
    pr_cond = side_conditions(pr_side, "name = 'ExecuteCompiler'")
    pr_rows = db.query(
        f"""SELECT file, max(dur) AS dur
        FROM build_time_trace
        WHERE {pr_cond}
        GROUP BY file"""
    )
    if not pr_rows:
        section.body = "No translation units were recompiled in this build (everything was served from the compiler cache)."
        return section
    pr_durs = {row["file"]: int(row["dur"]) for row in pr_rows}

    # The recompiled set can be the whole tree (a common-header PR), so the
    # file filter is a subquery rather than a literal IN list.
    base_rows = db.query(
        f"""SELECT file,
            argMax(dur, time) AS dur,
            argMax(commit_sha, time) AS sha,
            argMax(check_start_time, time) AS check_start_time,
            argMax(instance_id, time) AS instance_id
        FROM build_time_trace
        WHERE date >= today() - {TU_BASE_DAYS}
            AND pull_request_number = 0
            AND check_name = {quote(CHECK_NAME)}
            AND name = 'ExecuteCompiler'
            AND file IN (
                SELECT DISTINCT file
                FROM build_time_trace
                WHERE {pr_cond}
            )
            AND commit_sha IN ({in_list(master_shas)})
        GROUP BY file"""
    )
    # Carry the exact run (check_start_time, instance_id) that produced each
    # baseline duration, so drill_down_tu reads its entities from that same run
    # rather than re-resolving from the commit sha alone - a later rerun of the
    # same master commit that did not recompile this TU would otherwise pin the
    # drill-down to a run with no rows for this file, reporting every PR entity
    # as new.
    base_durs = {
        row["file"]: (
            int(row["dur"]),
            row["sha"],
            row["check_start_time"],
            row["instance_id"],
        )
        for row in base_rows
    }

    # Compile times of the PR build and of the (older, different-machine)
    # master baselines carry a uniform machine-speed skew: with enough matched
    # TUs the whole table shifts by the same ratio. The median ratio estimates
    # that skew; individual TUs are flagged only when they deviate from it, and
    # the skew itself is reported as an informational line (it can also be a
    # genuine everything-got-slower change, e.g. a heavy common header - that
    # still shows up, just not as per-TU findings).
    ratios = sorted(pr_durs[file] / max(base_dur, 1) for file, (base_dur, *_) in base_durs.items() if file in pr_durs)
    skew = ratios[len(ratios) // 2] if len(ratios) >= 10 else 1.0

    total_s = sum(pr_durs.values()) / 1e6
    findings = []
    for file, pr_dur in pr_durs.items():
        if file not in base_durs:
            continue
        base_dur, base_tu_sha, base_cst, base_iid = base_durs[file]
        base_tu_side = Side(TU_BASE_DAYS, 0, base_tu_sha, base_cst, base_iid)
        adjusted_base = base_dur * skew
        delta_s = (pr_dur - adjusted_base) / 1e6
        ratio = max(pr_dur, adjusted_base) / max(min(pr_dur, adjusted_base), 1)
        if abs(delta_s) >= TU_REPORT_SECONDS and ratio >= TU_REPORT_RATIO:
            findings.append((file, pr_dur, base_dur, base_tu_side, delta_s, ratio))
    findings.sort(key=lambda f: -abs(f[4]))

    lines = [f"{len(pr_durs)} translation units recompiled, {total_s:.0f} s compile time in total, {len(base_durs)} of them have a recent master baseline."]
    if abs(skew - 1.0) >= 0.05:
        lines += [
            "",
            f"Median compile-time ratio to the baselines is ×{skew:.2f} (machine-speed difference or a change affecting every TU); per-TU deltas below are relative to that ratio.",
        ]
    if findings:
        lines += [
            "",
            "| Translation unit | Master | PR | Δ vs median |",
            "|---|---:|---:|---:|",
        ]
        for file, pr_dur, base_dur, base_tu_side, delta_s, ratio in findings[:MAX_TABLE_ROWS]:
            if abs(delta_s) >= TU_SIG_SECONDS and ratio >= TU_SIG_RATIO:
                section.significant = True
            lines.append(f"| {md_code(strip_build_dir(file))} | {base_dur / 1e6:.1f} s | {pr_dur / 1e6:.1f} s | {format_seconds_delta(delta_s, base_dur * skew / 1e6)} |")
        section.summary = f"{len(findings)} translation units changed compile time"

        # Attribute the biggest slowdowns to concrete frontend/backend entities
        # (template instantiations, included headers, function codegen).
        for file, pr_dur, base_dur, base_tu_side, delta_s, ratio in findings[:3]:
            if delta_s <= 0:
                continue
            drill = drill_down_tu(db, pr_side, base_tu_side, file, skew)
            if drill:
                lines += ["", f"Slowest changed entities of {md_code(strip_build_dir(file))}:", ""]
                lines += drill

    # A brand-new TU has no baseline; surface it when it is expensive.
    new_tus = sorted(
        ((f, d) for f, d in pr_durs.items() if f not in base_durs and d >= 30e6),
        key=lambda x: -x[1],
    )
    if new_tus:
        lines += ["", "Expensive translation units without a recent master baseline:", ""]
        for file, dur in new_tus[:10]:
            lines.append(f"- {md_code(strip_build_dir(file))}: {dur / 1e6:.1f} s")
        # A large new compile-time cost is significant even without a baseline to
        # diff against; a missing baseline must not silence the top-level verdict.
        if any(dur >= TU_SIG_SECONDS * 1e6 for _, dur in new_tus):
            section.significant = True
            new_summary = f"{len(new_tus)} new translation units without a master baseline"
            section.summary = f"{section.summary}; {new_summary}" if section.summary else new_summary

    section.body = "\n".join(lines)
    return section


def drill_down_tu(db: Db, pr_side, base_side, file, skew=1.0) -> List[str]:
    """Top per-entity compile time changes inside one translation unit.

    Entity deltas are filtered against the same median machine-speed ratio as
    the per-TU comparison, so a uniformly slower run does not list every
    entity of the TU.

    `base_side` is the exact run that produced this TU's baseline compile time
    (carried out of `compare_compile_times` so the drill-down reads its entities
    from the same run, not from a later rerun of the same commit that may not
    have recompiled this TU).
    """
    rows = db.query(
        f"""SELECT name, detail,
            sumIf(dur, side = 'pr') AS pr_dur,
            sumIf(dur, side = 'base') AS base_dur,
            toInt64(pr_dur) - toInt64(base_dur) AS delta
        FROM {
            both_sides(
                "build_time_trace",
                "name, detail, dur",
                pr_side,
                base_side,
                f"file = {quote(file)} AND detail != '' AND name IN ('InstantiateFunction', 'InstantiateClass', 'ParseClass', 'Source', 'OptFunction', 'CodeGen Function')",
            )
        }
        GROUP BY name, detail
        HAVING abs(delta) >= 500000
        ORDER BY abs(delta) DESC
        LIMIT 40"""
    )
    lines = []
    for row in rows:
        pr_dur, base_dur = int(row["pr_dur"]), int(row["base_dur"])
        adjusted_base = base_dur * skew
        delta_s = (pr_dur - adjusted_base) / 1e6
        ratio = max(pr_dur, adjusted_base) / max(min(pr_dur, adjusted_base), 1)
        if abs(delta_s) < 0.5 or ratio < TU_REPORT_RATIO:
            continue
        lines.append(f"- {row['name']} {md_code(demangle(row['detail']))}: {format_seconds_delta(delta_s, adjusted_base / 1e6)}")
        if len(lines) >= 8:
            break
    return lines


def compare_symbols(db: Db, pr_side, base_side) -> Section:
    """Per-symbol size diff of the final linked binaries.

    Covers both `clickhouse` and `clickhouse-keeper` (the producer uploads the
    symbols of both), so a keeper-only size regression gets the same per-symbol
    attribution as the main binary. Each binary is compared only when both
    sides have its symbol data.
    """
    section = Section(title="Symbol sizes")
    sides = db.query(
        f"""SELECT file,
            countIf(side = 'pr') AS pr_count,
            countIf(side = 'base') AS base_count
        FROM {both_sides("binary_symbols", "file", pr_side, base_side, f"file IN ({in_list(LINK_BINARIES)})")}
        GROUP BY file"""
    )
    comparable = [row["file"] for row in sides if int(row["pr_count"]) and int(row["base_count"])]
    if not any(int(row["pr_count"]) for row in sides):
        return section
    if not comparable:
        section.body = "No symbol data for the master baseline yet (it is collected for release builds only since this check was introduced); the comparison will activate once master catches up."
        return section
    missing_base = [row["file"] for row in sides if int(row["pr_count"]) and not int(row["base_count"])]
    # size >= 1 KiB cuts the aggregation to a fraction of the ~1.5M symbols of
    # the binary; a symbol can only reach the report threshold if one side
    # exceeds it anyway.
    rows = db.query(
        f"""SELECT file, symbol,
            sumIf(size, side = 'pr') AS pr_size,
            sumIf(size, side = 'base') AS base_size,
            toInt64(pr_size) - toInt64(base_size) AS delta
        FROM {both_sides("binary_symbols", "file, symbol, size", pr_side, base_side, f"file IN ({in_list(comparable)}) AND size >= 1024")}
        GROUP BY file, symbol
        HAVING abs(delta) >= {SYMBOL_REPORT_BYTES}
        ORDER BY abs(delta) DESC
        LIMIT {MAX_TABLE_ROWS}"""
    )
    lines = []
    if missing_base:
        names = ", ".join(md_code(strip_build_dir(f)) for f in missing_base)
        lines.append(f"No master baseline symbol data yet for {names}.")
        lines.append("")
    if not rows:
        if not lines:
            return section
        section.body = "\n".join(lines).rstrip()
        return section
    lines += [
        "| Binary | Symbol | Master | PR | Δ |",
        "|---|---|---:|---:|---:|",
    ]
    for row in rows:
        pr_size, base_size = int(row["pr_size"]), int(row["base_size"])
        delta = int(row["delta"])
        if abs(delta) >= SYMBOL_SIG_BYTES:
            section.significant = True
        base_text = format_bytes(base_size) if base_size else "new"
        pr_text = format_bytes(pr_size) if pr_size else "removed"
        lines.append(f"| {md_code(strip_build_dir(row['file']))} | {md_code(row['symbol'])} | {base_text} | {pr_text} | {format_bytes_delta(delta, base_size)} |")
    section.body = "\n".join(lines)
    return section


def build_comment(info, pr_sha: str, base_sha: str, sections: List[Section]) -> str:
    significant = [s for s in sections if s.significant]
    repo_url = f"https://github.com/{info.repo_name}"
    lines = [
        f"### Build profile diff ({CHECK_NAME})",
        "",
        f"Comparing [`{pr_sha[:9]}`]({repo_url}/commit/{pr_sha}) with master "
        f"[`{base_sha[:9]}`]({repo_url}/commit/{base_sha}) "
        "(binary sizes, per-object sizes, per-symbol sizes, compile and link time).",
        "",
    ]
    if significant:
        titles = ", ".join(s.title.lower() for s in significant)
        lines.append(f"⚠️ **Significant changes: {titles}.**")
    else:
        lines.append("✅ No significant changes.")
    for section in sections:
        if not section.body:
            continue
        marker = " ⚠️" if section.significant else ""
        lines += [
            "",
            f"<details><summary><b>{section.title}</b>{marker}</summary>",
            "",
            section.body,
            "",
            "</details>",
        ]
    try:
        report_url = info.get_job_report_url(latest=False)
        if report_url:
            lines += ["", f"[Job report]({report_url})"]
    except Exception:
        pass
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="local run: no GH comment, print to stdout")
    parser.add_argument("--pr-sha", help="override the PR-side commit sha")
    parser.add_argument("--pr-number", type=int, help="override the PR number")
    parser.add_argument("--base-sha", help="override the baseline master sha")
    args = parser.parse_args()

    info = LocalInfo() if args.local else Info()
    pr_number = args.pr_number if args.pr_number is not None else info.pr_number
    pr_sha = args.pr_sha or info.sha
    if pr_number <= 0 and not args.local:
        Result.create_from(status=Result.Status.SKIPPED, info="Not a PR run").complete_job()
        return

    db = Db()

    if not has_pr_data(db, pr_number, pr_sha):
        info_text = f"No {CHECK_NAME} build profile data for commit {pr_sha} - the build was skipped, reused from cache, or predates profile upload"
        print(info_text)
        if args.local:
            return
        Result.create_from(status=Result.Status.OK, info=info_text).complete_job()
        return

    master_shas = get_master_shas(info)
    base_sha = args.base_sha or find_baseline(db, master_shas, pr_sha)
    if not base_sha:
        # Fail-close: no baseline means no comparison, not a comparison against
        # an arbitrary commit.
        raise RuntimeError("No master baseline with build profile data found - cannot compare")
    print(f"Comparing PR {pr_number} sha {pr_sha} against master {base_sha}")

    # Pin each side to one concrete build run once, and reuse it for every
    # table (see Side / resolve_run): the whole comparison then reflects a
    # single build instead of a per-table mix of reruns.
    pr_side = resolve_run(db, PR_DAYS, pr_number, pr_sha)
    base_side = resolve_run(db, BASE_DAYS, 0, base_sha)
    if pr_side is None or base_side is None:
        raise RuntimeError("Could not resolve a concrete build run for one of the sides")

    sections = [
        compare_binaries(db, pr_side, base_side),
        compare_objects(db, pr_side, base_side),
        compare_link_time(db, pr_side, base_side),
        compare_opt_functions(db, pr_side, base_side),
        compare_compile_times(db, pr_side, base_side, master_shas),
        compare_symbols(db, pr_side, base_side),
    ]

    body = build_comment(info, pr_sha, base_sha, sections)
    significant = [s for s in sections if s.significant]

    if args.local:
        print("=" * 80)
        print(body)
        return

    try:
        GH.post_updateable_comment(comment_tags_and_bodies={COMMENT_TAG: body})
    except Exception:
        # The comparison result is still in the job report; a GH hiccup should
        # not fail the check.
        print("WARNING: failed to post/update the PR comment")
        traceback.print_exc()

    summaries = [s.summary or s.title for s in significant]
    result_info = "Significant changes: " + "; ".join(summaries) if significant else f"No significant changes vs master {base_sha[:9]}"
    Result.create_from(status=Result.Status.OK, info=result_info).complete_job()


if __name__ == "__main__":
    main()
