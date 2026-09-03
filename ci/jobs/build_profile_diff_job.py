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
(reduced time trace, see LogClusterBuildProfileQueries.REDUCED_PROFILE_EVENTS).

Two master baselines are used, because a PR build and an official master build
are built with different flags: PR ThinLTO builds strip debug symbols
(-DDISABLE_ALL_DEBUG_SYMBOLS=1 in build_clickhouse.py) and skip the
official-build flag, so their object files, unstripped binaries and link times
are incomparable to master's (observed on a no-op PR as an apparent -73%
binary size and -39% link time):

  * `Build (arm_release_pr_cache_warmup)` - the master sccache-warmup build,
    compiled with exactly the PR flags but never linked. Baseline for object
    file sizes and per-TU compile times.
  * `Build (arm_release)` - the official master build. Baseline for the
    artifacts that do not depend on debug info: the stripped binary size and
    the per-symbol sizes of the linked binaries, plus per-function ThinLTO
    times (normalized by the median ratio to absorb the systematic flag and
    machine-speed skew). Unstripped binary sizes and link wall time are not
    compared at all.

If there is a significant change the check posts a PR comment (kept updated in
place on repeated runs) with the details; otherwise the comment states that
there are no significant changes.

Notes on data coverage:
  * PR and master builds use sccache, so `build_time_trace` contains compile
    events only for translation units that were actually recompiled. Per-TU
    compile times are therefore compared against the most recent warmup build
    that recompiled the same TU.
  * The ThinLTO link runs on every linking build, so per-function OptFunction
    events are present on both sides for every final binary that is actually
    linked (clickhouse-keeper has none when built as a symlink to clickhouse).
  * `binary_sizes` is complete on both sides; `binary_symbols` is complete on
    the PR side and on master builds since this check was introduced.
  * The warmup build builds every object-file target ninja knows about while a
    PR build builds only `clickhouse-bundle`, so the object-size comparison
    covers the object files the PR build produced (see `compare_objects`).
  * Names coming out of a ThinLTO link carry an unstable `.llvm.<hash>` clone
    suffix, so symbols and per-function times are keyed by the name with that
    suffix removed (see `strip_clone_suffix`).

Local run (no AWS SSM secrets and no PR comment; `gh` is still required, to
enumerate master history from the provided baseline):
  CI_LOGS_HOST=... CI_LOGS_PASSWORD=... CI_LOGS_USER=default \\
  python3 -m ci.jobs.build_profile_diff_job \\
      --local --pr-sha <sha> --pr-number <n> --base-sha <sha>
"""

import argparse
import dataclasses
import datetime
import json
import os
import statistics
import subprocess
import traceback
from typing import Dict, List, Optional

from ci.jobs.scripts.log_cluster import LogCluster
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

CHECK_NAME = "arm_release"
# The master sccache-warmup build: compiled with the PR build's exact cmake
# flags (see PR_CACHE_WARMUP_BUILD_TYPES in build_clickhouse.py), so it is the
# comparable baseline for everything measured before linking.
WARMUP_CHECK_NAME = "arm_release_pr_cache_warmup"
COMMENT_TAG = "build-profile-diff"

BUILD_DIR = "./ci/tmp/build"
MAIN_BINARY = f"{BUILD_DIR}/programs/clickhouse"
# Only the stripped binary is compared against the official master build: the
# unstripped binaries differ by the debug info that master keeps and PR builds
# strip, and the self-extracting binary is the compression of that difference.
HEADLINE_BINARIES = [
    f"{BUILD_DIR}/programs/clickhouse-stripped",
]
# The final linked binaries: both the per-symbol size diff and the per-function
# ThinLTO time diff cover each of them (clickhouse-keeper produces no data of
# its own when it is built as a symlink to clickhouse).
FINAL_BINARIES = [
    f"{BUILD_DIR}/programs/clickhouse",
    f"{BUILD_DIR}/programs/clickhouse-keeper",
]

# How far back to look for data. The baseline commit is at most hours old; the
# per-TU compile baseline needs a longer window because a TU enters the trace
# only when a master build actually recompiles it (sccache miss).
PR_DAYS = 7
BASE_DAYS = 7
TU_BASE_DAYS = 14
# How long after a master commit its profile can still arrive: the master-side
# windows end that far past their anchor. Over 673 master `Build (arm_release)`
# commits the upload lagged the commit by 4 hours at p99 and 85 hours at most.
UPLOAD_DELAY_DAYS = 4

# Significance thresholds. Object files are compared against the flag-identical
# warmup build, so their thresholds are tight. The stripped binary is compared
# against the official master build, which is compiled with debug info while a
# pull request build is not, and that leaks into the code itself (see
# XRAY_DEBUG_OFFSET_RATIO): a no-op pull request measured a -0.43% residual, so
# its threshold must absorb about that much. Times run on different machines
# under different load and need generous margins.
BINARY_SIG_BYTES = 8 << 20  # stripped binary: 8 MiB and
BINARY_SIG_RATIO = 0.01  # 1%
# The size by which the official master binary exceeds a pull request one for
# reasons no pull request can influence. A delta that lands on this offset is
# not shown at all, see compare_binaries.
#
# Pull request builds pass -DDISABLE_ALL_DEBUG_SYMBOLS=1 and the official master
# build does not (build_clickhouse.py), and `strip --strip-debug` does not undo
# the difference. XRay decides whether to instrument a loop-free function by
# counting MachineInstrs with debug pseudo-instructions included
# (`MICount += MBB.size()` in llvm/lib/CodeGen/XRayInstrumentation.cpp), so with
# debug info thousands of functions just under the 200-instruction threshold get
# entry/exit sleds that the pull request build never emits. Measured on master
# 9d8eed34c114 against pull request 116614: 11028 extra instrumented functions,
# 3.06 MiB of a 712 MiB stripped binary (0.43%), all of it in `xray_instr_map`,
# `xray_fn_idx`, the sled NOPs in `.text` and the `.Lxray_*` / `$d` symbols they
# add. The offset is always in the same direction, because the official build is
# the one carrying the extra sleds.
#
# The compiler side is fixed in llvm/llvm-project#219100; once that reaches the
# toolchain this offset can go away, and the headline row becomes exact again.
XRAY_DEBUG_OFFSET_RATIO = 0.0043
# How far a delta may sit from that offset and still be read as the offset. This
# is deliberately a window *around* the measurement rather than everything up to
# it: a pull request that grows the binary by less than the offset still compares
# smaller than master, and hiding the whole `[-offset, 0]` range would turn such
# a regression into a silent omission. At half the offset the window is
# +-1.53 MiB around -3.06 MiB, so any real change past that shows up, in either
# direction, and its upper edge stays below BINARY_SIG_RATIO.
XRAY_DEBUG_OFFSET_TOLERANCE = 0.5
# Shown whenever the headline size section says anything at all, so that a
# rendered negative delta is read with the offset in mind rather than as a size
# win of that size.
XRAY_DEBUG_OFFSET_NOTE = (
    "The official master build is compiled with `-g` and a pull request build is "
    "not, and XRay counts debug instructions towards its instrumentation "
    "threshold, so master instruments thousands of functions more and its binary "
    "is ~0.4% larger no matter what the pull request does."
)
OBJECT_REPORT_BYTES = 16 << 10  # .o file: report at 16 KiB,
OBJECT_SIG_BYTES = 256 << 10  # significant at 256 KiB
# Per-function ThinLTO time is the noisiest signal of the check: the two links
# run on different machines, with different flags, and the backend's per-function
# time depends on the import decisions of the whole module. Measured on pull
# requests that cannot possibly have changed the functions in question, single
# functions of a few seconds drifted by up to a factor of two - which the old
# 2 s / 1.5x reporting bar turned into a dozen-row table on every pull request.
OPTFN_REPORT_SECONDS = 5  # per-function LTO time: report at |delta| >= 5s
OPTFN_REPORT_RATIO = 2.0  # and 2x, significant at 20s
OPTFN_SIG_SECONDS = 20
TU_REPORT_SECONDS = 5  # per-TU compile time: report at |delta| >= 5s
TU_REPORT_RATIO = 1.3  # and 1.3x, significant at 20s and 1.5x
TU_SIG_SECONDS = 20
TU_SIG_RATIO = 1.5
# Section-wide compile-time shift: the median PR/baseline ratio is subtracted
# from every per-TU delta as machine-speed skew, so a change that moves every
# translation unit by the same factor produces no per-TU finding at all.
# It is judged on the section level instead, in both directions: a uniform
# shift of this ratio, costing or saving this much aggregate compile time
# across the matched translation units, is significant on its own. The margins
# are wide because both sides really do run on different machines - warmup
# baselines use the PR's flags, but not the PR's runner.
TU_SKEW_SIG_RATIO = 1.2
TU_SKEW_SIG_SECONDS = 300
# Per-symbol sizes are baselined on the official master build, not on the
# flag-identical warmup one: that build is compiled with debug info, and every
# function it instruments but a pull request build does not carries the sled NOPs
# in its own size (see XRAY_DEBUG_OFFSET_RATIO), so individual functions really
# do differ in size between two builds of the same source. That is worth ~3 MiB
# over the whole stripped binary, so the margins here are much wider than the
# object-file ones (those are baselined on a build compiled with the PR's exact
# flags).
SYMBOL_REPORT_BYTES = 64 << 10  # per-symbol size: report at 64 KiB,
SYMBOL_SIG_BYTES = 512 << 10  # significant at 512 KiB
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
        # This job only reads, so it goes to the read-only sub-service of the
        # CI logs cluster (LogCluster.READONLY_URL) rather than to the endpoint
        # that ingests the logs and profiles of the whole CI fleet.
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
                readonly=True,
            )
        else:
            self._cluster = LogCluster(readonly=True)

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


def recent_days(days: int) -> str:
    """A `date` condition covering the last `days` days up to today."""
    return f"date >= today() - {days}"


def anchored_window(anchor_date: datetime.date, days: int, today: datetime.date) -> str:
    """A `date` condition covering `days` before `anchor_date` up to its uploads.

    Closed on both ends: `date` leads the primary key of every profile table, so
    a bounded range prunes on it while an open `date >= start` scans every newer
    day of the whole fleet's telemetry. The upper end allows for profiles that
    arrive after their commit (UPLOAD_DELAY_DAYS) and never moves into the
    future, so an anchor at today reproduces `recent_days` exactly.
    """
    start = anchor_date - datetime.timedelta(days=days)
    end = min(anchor_date + datetime.timedelta(days=UPLOAD_DELAY_DAYS), today)
    return f"date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}'"


def master_windows(event_time: str, days_by_name: Dict[str, int]) -> Dict[str, str]:
    """The master-side `date` conditions, anchored on the run's own event.

    The baseline candidates are the frozen first-parent chain of the head's
    master parent, so a window measured from the wall clock excludes them all
    once the run is replayed later than the event it was triggered by (a rerun
    keeps the event payload, and with it `event_time`). Anchoring both on the
    event makes the candidate set and the row filter commensurable.

    An absent `event_time` (only `LocalInfo`, where `--base-sha` is mandatory
    and short-circuits the baseline lookup) keeps the wall-clock windows.
    """
    today = datetime.datetime.now(datetime.timezone.utc).date()
    if not event_time:
        return {name: recent_days(days) for name, days in days_by_name.items()}
    anchor = min(datetime.date.fromisoformat(event_time[:10]), today)
    return {name: anchored_window(anchor, days, today) for name, days in days_by_name.items()}


def walk_cutoff(event_time: str, days: int) -> str:
    """The ISO 8601 UTC timestamp the first-parent walk stops at.

    Reaches past the lower bound of the anchored window: that bound is a whole
    calendar upload day, and an upload lags its commit, so a commit older than
    the bound can still own a row inside the window.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    anchor = now
    if event_time:
        parsed = datetime.datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        anchor = min(parsed, now)
    start = anchor.date() - datetime.timedelta(days=days + UPLOAD_DELAY_DAYS)
    return f"{start.isoformat()}T00:00:00Z"


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

    date_condition: str
    pr_number: int
    sha: str
    check_start_time: str
    instance_id: str
    check_name: str = CHECK_NAME


def resolve_run(
    db: "Db",
    date_condition: str,
    pr_number: int,
    sha: str,
    table: str = "binary_sizes",
    check_name: str = CHECK_NAME,
) -> Optional[Side]:
    """The newest uploaded build run for one side, or None if there is no data.

    Resolved from `binary_sizes` by default (the canonical, always-complete
    table); the resulting (check_start_time, instance_id) is reused for every
    other table of the same build.
    """
    rows = db.query(
        f"""SELECT check_start_time, instance_id
        FROM {table}
        WHERE {date_condition}
            AND pull_request_number = {pr_number}
            AND commit_sha = {quote(sha)}
            AND check_name = {quote(check_name)}
        ORDER BY check_start_time DESC
        LIMIT 1"""
    )
    if not rows:
        return None
    return Side(date_condition, pr_number, sha, rows[0]["check_start_time"], rows[0]["instance_id"], check_name)


def side_conditions(side: Side, extra_where: str = "") -> str:
    """The WHERE conditions selecting one side's rows, pinned to its build run."""
    where = f"\n            AND {extra_where}" if extra_where else ""
    return f"""{side.date_condition}
            AND pull_request_number = {side.pr_number}
            AND commit_sha = {quote(side.sha)}
            AND check_name = {quote(side.check_name)}
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


# ThinLTO's clone suffix, in the demangled form (` [clone .llvm.123]`) and in the
# mangled one (`.llvm.123`) - `binary_symbols` holds demangled names, the time
# trace holds mangled ones.
CLONE_SUFFIX_RE = r"' ?\\[clone \\.llvm\\.[0-9]+\\]'"
LLVM_SUFFIX_RE = r"'\\.llvm\\.[0-9]+'"


def strip_clone_suffix(column: str) -> str:
    """SQL dropping ThinLTO's `.llvm.<hash>` suffix from a symbol name.

    When ThinLTO imports a function it promotes the module-local symbols it
    needs and renames them with a `.llvm.<hash>` suffix (`nm --demangle` renders
    it as ` [clone .llvm.<hash>]`). The hash is derived from the defining
    module's identity, so it is not stable across builds: two builds of the very
    same source name the same clone differently.

    Both sides are therefore normalized by dropping the suffix, which also folds
    the several clones of one function into a single row. Without it an entirely
    unchanged function appears twice - once as removed, once as new, at exactly
    the same size - and those phantom pairs were the largest rows of the symbol
    and ThinLTO tables on every pull request, and enough to declare both
    sections significant.
    """
    without_clone = f"replaceRegexpAll({column}, {CLONE_SUFFIX_RE}, '')"
    return f"replaceRegexpAll({without_clone}, {LLVM_SUFFIX_RE}, '')"


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
    event_time = ""

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
    already stores ~100 commits, so it needs no more. The per-TU compile
    baseline is different: it looks back TU_BASE_DAYS, far past ~100 commits
    of master history - `extend_master_shas` pages the older ancestors for it.

    Fail-close: the anchored chain is the only source in CI. `master_commits`
    (the global master tip) is deliberately not used as a fallback - it is not
    anchored to the PR's merge base, so it can offer a baseline commit that is
    newer than the master the PR was built on and attribute unrelated master
    changes to the PR. An absent chain returns an empty list, and the caller
    fails the job (a local run seeds the chain from `--base-sha` instead).
    """
    seen = set()
    shas = []
    for sha in list(info.get_kv_data("master_track_commits_sha") or []):
        if sha not in seen:
            seen.add(sha)
            shas.append(sha)
    return shas


# How many 100-commit listing fetches the first-parent walk performs at most.
# `repos/.../commits` interleaves merged PRs' own commits with the master
# merge commits, so one 100-commit page typically advances the first-parent
# chain by only ~25-50 commits. ClickHouse master merges up to ~100 commits a
# day, so 60 fetches cover the walk's TU_BASE_DAYS + UPLOAD_DELAY_DAYS horizon
# with margin (measured: 21 fetches for 19 days over 1247 first-parent commits).
EXTEND_MAX_PAGES = 60


def _list_commits_page(anchor_sha: str, page: int) -> List[dict]:
    """One page of the commits reachable from `anchor_sha`, newest first.

    This is NOT the first-parent chain: the listing interleaves merged PRs'
    second-parent commits. Each entry carries its parent shas so that
    `_walk_first_parent` can reconstruct the chain client-side.
    """
    out = subprocess.run(
        [
            "gh",
            "api",
            # Hardcoded upstream namespace, like the store_data hook: the
            # profile rows in the CI logs cluster carry public-repo shas.
            f"repos/ClickHouse/ClickHouse/commits?sha={anchor_sha}&per_page=100&page={page}",
            "--jq",
            "[.[] | {sha: .sha, date: .commit.committer.date, parents: [.parents[].sha]}]",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    ).stdout
    return json.loads(out)


def _walk_first_parent(anchor_sha: str, cutoff: str, max_pages: int, list_page, max_commits: int = 0):
    """The first-parent chain from `anchor_sha`, newest first.

    `repos/.../commits?sha=...` lists ALL commits reachable from the anchor,
    merged PRs' second-parent commits included, so the listing itself must not
    be taken for the master chain: the extra commits burn the page budget, and
    their commit dates - set when the PR branch was authored, arbitrarily far
    in the past - would fire a date cutoff long before the first-parent
    history reaches it. The chain is therefore reconstructed by following
    `parents[0]` through the fetched pages, and every fetch is re-anchored at
    the first first-parent sha not fetched yet, so each fetch is guaranteed to
    advance the walk (a listing starts with its own anchor).

    The walk stops once the chain's own tail crosses `cutoff` (ISO 8601 UTC,
    compares lexicographically), reaches the root, or - when `max_commits` is
    positive - reaches that length. Returns `(chain, complete)`: `complete` is
    False when the fetch budget ran out first.
    """
    commits: dict = {}
    chain: List[str] = []
    wanted = anchor_sha
    pages = 0
    while True:
        while wanted in commits:
            commit = commits[wanted]
            chain.append(wanted)
            if commit["date"] < cutoff or not commit["parents"] or len(chain) == max_commits:
                return chain, True
            wanted = commit["parents"][0]
        if pages >= max_pages:
            return chain, False
        pages += 1
        for commit in list_page(wanted, 1):
            commits.setdefault(commit["sha"], commit)
        if wanted not in commits:
            # Fail-close, like a failing fetch: a listing that does not start
            # with its own anchor cannot anchor a baseline chain.
            raise RuntimeError(f"the commit listing anchored at {wanted} does not contain it")


def extend_master_shas(master_shas: List[str], cutoff: str, list_page=_list_commits_page) -> List[str]:
    """Extend the anchored chain far enough back to cover the per-TU window.

    `master_track_commits_sha` holds only ~100 first-parent commits - a day or
    two of master - while the per-TU compile baseline advertises TU_BASE_DAYS:
    a warmup trace inside that window but older than the chain's tail would be
    invisible to `compare_compile_times`. Walk the first-parent history from
    the chain's oldest commit (`_walk_first_parent`), so every added sha is a
    master commit and an ancestor of the PR's merge base (a baseline must
    never contain changes the PR does not have).

    `cutoff` comes from the same anchor as the per-TU SQL window and reaches
    further back than it, so the returned chain is a superset of the commits
    that window can return a row for.

    Fail-close: a GitHub API failure propagates and fails the job (which is
    `allow_failure`) instead of degrading to the un-extended ~100-sha chain.
    The shallow chain would silently hide valid 8-14 day warmup baselines,
    turning a transient API hiccup into a false-green compile-time comparison
    (TUs misreported as new, or the whole section as the catch-up note).
    """
    if not master_shas:
        return master_shas
    chain, complete = _walk_first_parent(master_shas[-1], cutoff, EXTEND_MAX_PAGES, list_page)
    seen = set(master_shas)
    shas = list(master_shas)
    for sha in chain:
        if sha not in seen:
            seen.add(sha)
            shas.append(sha)
    if not complete:
        # Hitting the fetch cap is a bounded, loud partial (well past the
        # window under normal merge rates), unlike the unbounded API failure.
        print(f"WARNING: the master chain still does not reach {cutoff} after {EXTEND_MAX_PAGES} pages ({len(shas)} commits)")
    return shas


def seed_master_shas(anchor_sha: str, list_page=_list_commits_page) -> List[str]:
    """The master chain for a local run, anchored at the provided baseline.

    Local runs have no `master_track_commits_sha` kv metadata (`LocalInfo`
    carries none), so the chain is seeded with the first ~100 first-parent
    ancestors of the `--base-sha` commit from the GitHub API - the same shape,
    and the same anchoring guarantee (every sha is a master commit reachable
    from the baseline), that the store_data hook records in CI.
    `extend_master_shas` then grows it further back for the per-TU window as
    usual.
    """
    chain, complete = _walk_first_parent(anchor_sha, "0", EXTEND_MAX_PAGES, list_page, max_commits=100)
    if not complete:
        print(f"WARNING: the seeded master chain holds only {len(chain)} commits after {EXTEND_MAX_PAGES} pages")
    return chain


def find_baseline(db: Db, master_shas: List[str], pr_sha: str, date_condition: str) -> Optional[str]:
    """The most recent master commit with uploaded arm_release profile data.

    The PR-side commit is excluded so that a re-run on an already-merged
    commit does not compare it against itself.

    A `binary_sizes` row for MAIN_BINARY means the whole profile of that
    commit is there, not just its sizes: the producer uploads binary_sizes.txt
    last precisely so that it marks a complete upload (see _UPLOAD_ORDER in
    ci/jobs/scripts/job_hooks/build_profile_hook.py). Without that ordering a
    master build that died after its sizes but before its symbols would become
    the canonical baseline and quietly cost every PR its symbol section.
    """
    candidates = [sha for sha in master_shas if sha != pr_sha][:100]
    if not candidates:
        return None
    rows = db.query(
        f"""SELECT DISTINCT commit_sha
        FROM binary_sizes
        WHERE {date_condition}
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


def find_warmup_baseline(db: Db, master_shas: List[str], pr_sha: str, date_condition: str) -> Optional[str]:
    """The most recent master commit with uploaded warmup-build profile data.

    The warmup build compiles with the PR flags but does not link, so its
    canonical data is the object files, not MAIN_BINARY. May legitimately be
    absent while master catches up with profiling the warmup build.

    As in `find_baseline`, the `binary_sizes` row is the completion marker: it
    is uploaded after the time trace, so a warmup commit selected here also has
    the per-TU compile times the drill-down reads.
    """
    candidates = [sha for sha in master_shas if sha != pr_sha][:100]
    if not candidates:
        return None
    rows = db.query(
        f"""SELECT DISTINCT commit_sha
        FROM binary_sizes
        WHERE {date_condition}
            AND pull_request_number = 0
            AND check_name = {quote(WARMUP_CHECK_NAME)}
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
        WHERE {recent_days(PR_DAYS)}
            AND pull_request_number = {pr_number}
            AND commit_sha = {quote(pr_sha)}
            AND check_name = {quote(CHECK_NAME)}
            AND file = {quote(MAIN_BINARY)}"""
    )
    return rows and int(rows[0]["c"]) > 0


def compare_binaries(db: Db, pr_side, base_side) -> Section:
    """Size of the stripped binary vs the official master build.

    Only the stripped binary is comparable: master keeps debug symbols while
    PR ThinLTO builds strip them (build_clickhouse.py), so the unstripped and
    self-extracting binaries differ by gigabytes on any PR.

    Even the stripped binary carries a fixed offset in master's favour, because
    debug info leaks into codegen through XRay's instruction threshold (see
    XRAY_DEBUG_OFFSET_RATIO): the official build instruments ~11k functions the
    pull request build leaves alone, worth ~0.43% of the binary. That is not
    something a pull request can influence, and reporting it as a -3 MiB change
    on every pull request only invites a hunt for a size win that does not
    exist - so a delta that lands on the offset is not shown at all, just named.
    The window is centred on the measured offset rather than reaching up to it
    (XRAY_DEBUG_OFFSET_TOLERANCE), so a pull request that grows the binary while
    still comparing smaller than master is reported rather than swallowed, and a
    delta large enough to be significant is always shown.

    This does hide a genuine change of exactly the offset's size, and there is no
    way around that while the baseline is the official build: a 3 MiB saving and
    the offset are indistinguishable here. Recovering it needs a baseline
    compiled with the pull request's flags - the warmup build, once it links a
    binary of its own - or the offset gone from the compiler.

    This is the check's headline size signal, so it must never disappear
    silently: a headline binary whose size row the PR build did not upload
    (while the run itself resolved via the main binary's rows) means the
    profile producer lost it, and is flagged as an incomplete comparison
    instead of an all-green omission. A row the master baseline lacks is
    called out as a missing baseline.
    """
    section = Section(title="Binary sizes")
    rows = db.query(
        f"""SELECT file,
            maxIf(size, side = 'pr') AS pr_size,
            maxIf(size, side = 'base') AS base_size
        FROM {both_sides("binary_sizes", "file, size", pr_side, base_side, f"file IN ({in_list(HEADLINE_BINARIES)})")}
        GROUP BY file
        ORDER BY file"""
    )
    sizes = {row["file"]: (int(row["pr_size"]), int(row["base_size"])) for row in rows}
    missing_pr = [f for f in HEADLINE_BINARIES if not sizes.get(f, (0, 0))[0]]
    missing_base = [f for f in HEADLINE_BINARIES if sizes.get(f, (0, 0))[0] and not sizes[f][1]]
    lines = []
    summaries = []
    if missing_pr:
        names = ", ".join(md_code(strip_build_dir(f)) for f in missing_pr)
        lines.append(
            f"⚠️ This build uploaded no size row for {names}: the profile "
            "producer lost it and the headline size comparison is incomplete."
        )
        lines.append("")
        section.significant = True
        summaries.append(f"missing PR-side size data for {', '.join(strip_build_dir(f) for f in missing_pr)}")
    if missing_base:
        names = ", ".join(md_code(strip_build_dir(f)) for f in missing_base)
        lines.append(f"No master baseline size data yet for {names}.")
        lines.append("")
    table = [
        "| Binary | Master | PR | Δ |",
        "|---|---:|---:|---:|",
    ]
    within_offset = []
    for file in HEADLINE_BINARIES:
        pr_size, base_size = sizes.get(file, (0, 0))
        if not pr_size or not base_size:
            continue
        delta = pr_size - base_size
        offset = base_size * XRAY_DEBUG_OFFSET_RATIO
        name = strip_build_dir(file)
        if abs(delta) >= BINARY_SIG_BYTES and abs(delta) >= base_size * BINARY_SIG_RATIO:
            section.significant = True
            summaries.append(f"{name}: {format_bytes_delta(delta, base_size)}")
        elif (
            -offset * (1 + XRAY_DEBUG_OFFSET_TOLERANCE)
            <= delta
            <= -offset * (1 - XRAY_DEBUG_OFFSET_TOLERANCE)
        ):
            # The known debug-info/XRay offset, not the pull request. Checked
            # after significance, so a flagged delta is never hidden by it.
            within_offset.append(name)
            continue
        table.append(f"| {md_code(name)} | {format_bytes(base_size)} | {format_bytes(pr_size)} | {format_bytes_delta(delta, base_size)} |")
    if len(table) > 2:
        table.append("")
        table.append(
            "Only the stripped binary is compared: the official master build keeps "
            "debug symbols while PR builds strip them, so the other binaries differ "
            "by construction."
        )
        lines += table
    if within_offset:
        if lines:
            lines.append("")
        names = ", ".join(md_code(name) for name in within_offset)
        lines.append(
            f"{names}: smaller than the master baseline by the known offset "
            "between the two builds, so the difference is not shown. A delta "
            f"that differs from the offset by more than {XRAY_DEBUG_OFFSET_TOLERANCE:.0%} "
            "of it is shown, in either direction."
        )
    if len(table) > 2 or within_offset:
        lines += ["", XRAY_DEBUG_OFFSET_NOTE]
    section.body = "\n".join(lines).rstrip()
    section.summary = "; ".join(summaries)
    return section


# Object files that exist in every build under a different random path: cmake
# feature-test scratch objects (kept by --debug-trycompile) and Rust
# incremental-compilation artifacts. They are not part of the product and would
# show up as added/removed churn in every comparison.
OBJECT_FILTER = "file LIKE '%.o' AND file NOT LIKE '%CMakeScratch%' AND file NOT LIKE '%/incremental/%'"


WARMUP_CATCHUP_NOTE = (
    "No master baseline from the warmup build yet (it is the only master build "
    "compiled with the PR's flags, profiled since this check was introduced); "
    "the comparison will activate once master catches up."
)


def compare_objects(db: Db, pr_side, base_side) -> Section:
    """Per-object sizes vs the master warmup build (`base_side`).

    The warmup build is the only master build compiled with the PR's exact
    flags: the official master build keeps debug symbols inside every .o file
    while PR builds strip them, which would dwarf any real change.

    Only the object files the PR build produced are compared. The two builds do
    not have the same target set: the warmup build compiles every object-file
    target ninja knows about (see PR_CACHE_WARMUP_BUILD_TYPES in
    build_clickhouse.py), while a PR build only builds `clickhouse-bundle`, so
    hundreds of object files - `grpc_unsecure`, protobuf-lite, the gRPC half of
    google-cloud-cpp, the unit tests, the utils - exist on the warmup side alone.
    Reading them as removals produced a "685 removed, -40 MiB" finding on every
    pull request. A PR-only object file, on the other hand, is a real addition:
    the warmup side builds a superset of the target set, so it is a source file
    the PR added. The reverse signal - a source file the PR deletes - is left to
    the binary and symbol sections; there is no way to tell it apart from the
    target-set difference here.
    """
    section = Section(title="Object file sizes")
    if base_side is None:
        section.body = WARMUP_CATCHUP_NOTE
        return section
    rows = db.query(
        f"""SELECT file,
            maxIf(size, side = 'pr') AS pr_size,
            maxIf(size, side = 'base') AS base_size,
            toInt64(pr_size) - toInt64(base_size) AS delta
        FROM {both_sides("binary_sizes", "file, size", pr_side, base_side, OBJECT_FILTER)}
        GROUP BY file
        HAVING pr_size > 0 AND abs(delta) >= {OBJECT_REPORT_BYTES}
        ORDER BY abs(delta) DESC
        LIMIT {MAX_TABLE_ROWS}"""
    )
    totals = db.query(
        f"""SELECT
            countIf(pr_size > 0 AND base_size > 0 AND pr_size != base_size) AS changed,
            countIf(pr_size > 0 AND base_size = 0) AS added,
            countIf(pr_size = 0) AS base_only,
            sumIf(toInt64(pr_size) - toInt64(base_size), pr_size > 0) AS total_delta
        FROM (
            SELECT file,
                maxIf(size, side = 'pr') AS pr_size,
                maxIf(size, side = 'base') AS base_size
            FROM {both_sides("binary_sizes", "file, size", pr_side, base_side, OBJECT_FILTER)}
            GROUP BY file
        )"""
    )[0]
    changed, added, base_only = (
        int(totals["changed"]),
        int(totals["added"]),
        int(totals["base_only"]),
    )
    # The baseline-only files are the target-set difference, not a finding, so
    # they never bring the section to life on their own. Say how many were left
    # out in the job log even when the section stays silent.
    if base_only:
        print(f"{base_only} object files exist only in the warmup baseline (target-set difference) and are not compared")
    if not rows and not added:
        return section

    lines = [
        f"{changed} object files changed ({format_bytes_delta(int(totals['total_delta']), 0)} total), {added} added.",
        "",
        "| Object file | Master | PR | Δ |",
        "|---|---:|---:|---:|",
    ]
    for row in rows:
        pr_size, base_size = int(row["pr_size"]), int(row["base_size"])
        delta = int(row["delta"])
        # OBJECT_FILTER keeps only real compile-stage .o files (link-stage
        # artifacts do not end in .o, and scratch/incremental dirs are
        # excluded), and the warmup build compiles a superset of the PR's
        # targets with the same flags - so a PR-only row is a genuinely added
        # object file. Its whole size is the delta, and it drives the verdict
        # exactly like a size change of a file present on both sides.
        if abs(delta) >= OBJECT_SIG_BYTES:
            section.significant = True
        base_text = format_bytes(base_size) if base_size else "new"
        lines.append(f"| {md_code(strip_build_dir(row['file']))} | {base_text} | {format_bytes(pr_size)} | {format_bytes_delta(delta, base_size)} |")
    if base_only:
        lines += [
            "",
            f"{base_only} more object {'file is' if base_only == 1 else 'files are'} built by the "
            "master warmup baseline only (it builds every object-file target, a "
            "pull request build only `clickhouse-bundle`) and not compared.",
        ]
    section.body = "\n".join(lines)
    section.summary = f"{changed} object files changed, {added} added"
    return section


def compare_opt_functions(db: Db, pr_side, base_side) -> Section:
    """Per-function ThinLTO optimization time of the final linked binaries.

    This is the per-function 'how long does the backend chew on it' signal; it
    is present on every linking build (the link is never cached) and it is
    where a new heavyweight function or template instantiation shows up first.
    It covers every binary in FINAL_BINARIES, keyed by (binary, function), so
    a keeper-only PR cannot regress the `clickhouse-keeper` ThinLTO time
    without showing up here. Like the symbol section, a binary is compared
    only when both sides have its link trace (`clickhouse-keeper` produces
    none when built as a symlink to `clickhouse`), and a PR-side binary
    without a master baseline is called out instead of silently dropped. The
    reverse - a baseline exists but the PR build uploaded no link trace for
    that binary - means the profile producer lost rows (the build type, and
    with it the symlinked-keeper layout, is the same on both sides), so it is
    flagged as an incomplete comparison instead of an all-green omission.

    The baseline is the official master build, which runs on a different
    machine and with different flags (debug info, official-build flag,
    PGO/BOLT availability) - that skews every function's time by a roughly
    uniform ratio. The median ratio over matched functions estimates the skew
    and per-function deltas are taken relative to it, the same way the per-TU
    compile-time section normalizes its machine-speed skew.

    The skew is estimated per binary. Each binary is a separate ThinLTO link, so
    the two links of a build do not even run at the same time, let alone the
    links of the two sides: their ratios to the baseline routinely differ by more
    than a factor of two. One median over both binaries splits the difference and
    charges the whole gap to the functions of both - it reported the unchanged
    functions of `clickhouse` as several seconds faster and those of
    `clickhouse-keeper` as twice as slow, in the same table, on a pull request
    that touched neither.

    A shift that moves every function of a binary by the same factor is the skew
    itself, so - unlike the per-TU compile-time section, which judges its skew on
    the section level - it produces no finding here. Absolute link times are not
    comparable between the two sides at all (the master build's debug info alone
    changes them by tens of percent), so there is nothing to judge such a shift
    against.
    """
    section = Section(title="Slowest function optimization changes (ThinLTO)")
    trace_where = "name = 'OptFunction' AND dur >= 50000"
    # Per-binary trace presence must be judged on unfiltered OptFunction rows:
    # a binary whose complete link trace stays below the 50 ms reporting
    # cutoff on one side (realistic for clickhouse-keeper) is an intact
    # upload, not a lost one. The count-only aggregation is cheap; the cutoff
    # applies only to the expensive diff aggregations below.
    sides = db.query(
        f"""SELECT file,
            countIf(side = 'pr') AS pr_count,
            countIf(side = 'base') AS base_count
        FROM {both_sides("build_time_trace", "file", pr_side, base_side, f"file IN ({in_list(FINAL_BINARIES)}) AND name = 'OptFunction'")}
        GROUP BY file"""
    )
    comparable = [row["file"] for row in sides if int(row["pr_count"]) and int(row["base_count"])]
    missing_base = [row["file"] for row in sides if int(row["pr_count"]) and not int(row["base_count"])]
    missing_pr = [row["file"] for row in sides if int(row["base_count"]) and not int(row["pr_count"])]
    lines = []
    if missing_pr:
        # The master baseline has a link trace for this binary but the PR
        # build uploaded none. Both sides run the same build type, so the
        # symlinked-keeper case cannot differ between them: the PR-side
        # profile producer lost the rows. Never render this as all-green.
        names = ", ".join(md_code(strip_build_dir(f)) for f in missing_pr)
        lines.append(
            f"⚠️ The master baseline has a link trace for {names} but this build "
            "uploaded none: the profile producer lost it and the comparison is incomplete."
        )
        lines.append("")
        section.significant = True
        section.summary = f"missing PR-side link trace for {', '.join(strip_build_dir(f) for f in missing_pr)}"
    if missing_base:
        names = ", ".join(md_code(strip_build_dir(f)) for f in missing_base)
        lines.append(f"No master baseline link trace for {names}; its functions are not compared.")
        lines.append("")
    if not comparable:
        section.body = "\n".join(lines).rstrip()
        return section
    where = f"file IN ({in_list(comparable)}) AND {trace_where}"
    # ThinLTO renames the clones it creates with an unstable hash, so both sides
    # are keyed by the normalized function name (see strip_clone_suffix).
    both = both_sides(
        "build_time_trace",
        f"file, {strip_clone_suffix('detail')} AS detail, dur",
        pr_side,
        base_side,
        where,
    )
    # The systematic skew between the sides, as the median PR/master time
    # ratio over functions matched on both sides with a non-trivial baseline,
    # per binary (each is its own link, see above).
    # Interpolated: `medianExact` returns the upper middle element on an even
    # count, which overestimates the shift when a part of the functions
    # regressed and would normalize that regression away.
    skew_rows = db.query(
        f"""SELECT file, quantileExactWeightedInterpolated(0.5)(pr_dur / base_dur, 1) AS skew, count() AS matched
        FROM (
            SELECT file, detail,
                sumIf(dur, side = 'pr') AS pr_dur,
                sumIf(dur, side = 'base') AS base_dur
            FROM {both}
            GROUP BY file, detail
            HAVING pr_dur >= 500000 AND base_dur >= 500000
        )
        GROUP BY file"""
    )
    skews = {row["file"]: float(row["skew"]) for row in skew_rows if int(row["matched"]) >= 10}
    skew_expr = "1.0"
    if skews:
        skew_expr = f"transform(file, [{in_list(skews)}], [{', '.join(f'{v}' for v in skews.values())}], 1.0)"
    report_us = int(OPTFN_REPORT_SECONDS * 1e6)
    # dur >= 50ms cuts the aggregation from ~1M rows per side to tens of
    # thousands; a function can only reach the report threshold if one side
    # exceeds it anyway, and a missing side renders as "new"/"gone".
    rows = db.query(
        f"""SELECT file, detail,
            sumIf(dur, side = 'pr') AS pr_dur,
            sumIf(dur, side = 'base') AS base_dur,
            base_dur * ({skew_expr}) AS adjusted_base_dur,
            toInt64(pr_dur) - toInt64(adjusted_base_dur) AS delta
        FROM {both}
        GROUP BY file, detail
        HAVING abs(delta) >= {report_us}
            AND (pr_dur = 0 OR base_dur = 0
                 OR greatest(toFloat64(pr_dur), adjusted_base_dur) >= least(toFloat64(pr_dur), adjusted_base_dur) * {OPTFN_REPORT_RATIO})
        ORDER BY abs(delta) DESC
        LIMIT {MAX_TABLE_ROWS}"""
    )
    if not rows:
        section.body = "\n".join(lines).rstrip()
        return section
    skewed = sorted((f, s) for f, s in skews.items() if abs(s - 1.0) >= 0.05)
    if skewed:
        ratios = ", ".join(f"{md_code(strip_build_dir(f))} ×{s:.2f}" for f, s in skewed)
        lines += [
            f"Median per-function time ratio to the master baseline: {ratios} "
            "(each binary is linked separately, on a different machine and with "
            "different build flags); deltas below are relative to it.",
            "",
        ]
    lines += [
        "| Binary | Function | Master | PR | Δ vs median |",
        "|---|---|---:|---:|---:|",
    ]
    for row in rows:
        pr_s, base_s = int(row["pr_dur"]) / 1e6, int(row["base_dur"]) / 1e6
        adjusted_base_s = base_s * skews.get(row["file"], 1.0)
        delta = pr_s - adjusted_base_s
        if abs(delta) >= OPTFN_SIG_SECONDS:
            section.significant = True
        base_text = f"{base_s:.1f} s" if base_s else "new"
        pr_text = f"{pr_s:.1f} s" if pr_s else "gone"
        lines.append(f"| {md_code(strip_build_dir(row['file']))} | {md_code(demangle(row['detail']))} | {base_text} | {pr_text} | {format_seconds_delta(delta, adjusted_base_s)} |")
    section.body = "\n".join(lines)
    return section


def compare_compile_times(db: Db, pr_side, master_shas, date_condition: str) -> Section:
    """Per-TU compile time of TUs this PR recompiled.

    sccache makes the recompiled set exactly the TUs affected by the PR. Each
    is compared against the most recent master *warmup* build that also
    recompiled it - the warmup build compiles with the PR's exact flags, so
    unlike the official master build (which emits debug info) its compile
    times are directly comparable.

    Warmup availability is resolved here, over this comparison's own
    TU_BASE_DAYS window - not from the BASE_DAYS object-size baseline, whose
    shorter window would suppress this whole section while usable (8-14 days
    old) warmup traces still exist.

    Both sides are keyed by (file, library), not file alone: the same source
    path can be compiled more than once per build - with
    BUILD_STANDALONE_KEEPER=1, `programs/keeper/Keeper.cpp` is built into both
    `clickhouse-keeper-lib` and the standalone `clickhouse-keeper` - and
    `prepare-time-trace.sh` keeps the target name only in `library`. Keying by
    file alone would collapse those distinct compile jobs into one pseudo-TU
    and compare the PR's standalone compile against the master's library one.
    """
    section = Section(title="Compile time of recompiled translation units")
    pr_cond = side_conditions(pr_side, "name = 'ExecuteCompiler'")
    pr_rows = db.query(
        f"""SELECT file, library, max(dur) AS dur
        FROM build_time_trace
        WHERE {pr_cond}
        GROUP BY file, library"""
    )
    if not pr_rows:
        section.body = "No translation units were recompiled in this build (everything was served from the compiler cache)."
        return section
    pr_durs = {(row["file"], row["library"]): int(row["dur"]) for row in pr_rows}

    # A file compiled into several targets is ambiguous by name alone: label
    # such TUs with their library so the two compile jobs stay tellable apart.
    file_lib_counts: Dict[str, int] = {}
    for file, _library in pr_durs:
        file_lib_counts[file] = file_lib_counts.get(file, 0) + 1

    def tu_label(file: str, library: str) -> str:
        if library and file_lib_counts.get(file, 0) > 1:
            return f"{strip_build_dir(file)} ({library})"
        return strip_build_dir(file)

    # The recompiled set can be the whole tree (a common-header PR), so the
    # file filter is a subquery rather than a literal IN list.
    base_rows = db.query(
        f"""SELECT file, library,
            argMax(dur, time) AS dur,
            argMax(commit_sha, time) AS sha,
            argMax(check_start_time, time) AS check_start_time,
            argMax(instance_id, time) AS instance_id
        FROM build_time_trace
        WHERE {date_condition}
            AND pull_request_number = 0
            AND check_name = {quote(WARMUP_CHECK_NAME)}
            AND name = 'ExecuteCompiler'
            AND (file, library) IN (
                SELECT DISTINCT file, library
                FROM build_time_trace
                WHERE {pr_cond}
            )
            AND commit_sha IN ({in_list(master_shas)})
        GROUP BY file, library"""
    )
    # Carry the exact run (check_start_time, instance_id) that produced each
    # baseline duration, so drill_down_tu reads its entities from that same run
    # rather than re-resolving from the commit sha alone - a later rerun of the
    # same master commit that did not recompile this TU would otherwise pin the
    # drill-down to a run with no rows for this file, reporting every PR entity
    # as new.
    base_durs = {
        (row["file"], row["library"]): (
            int(row["dur"]),
            row["sha"],
            row["check_start_time"],
            row["instance_id"],
        )
        for row in base_rows
    }

    # No baseline for any recompiled TU can mean two very different things: no
    # warmup trace data at all (master has not caught up profiling the warmup
    # build - degrade to the catch-up note), or warmup data exists but none of
    # it covers this PR's TUs (a legitimate comparison where every TU is new).
    if not base_durs:
        any_warmup = db.query(
            f"""SELECT count() AS c
            FROM build_time_trace
            WHERE {date_condition}
                AND pull_request_number = 0
                AND check_name = {quote(WARMUP_CHECK_NAME)}
                AND name = 'ExecuteCompiler'
                AND commit_sha IN ({in_list(master_shas)})"""
        )
        if not (any_warmup and int(any_warmup[0]["c"]) > 0):
            section.body = WARMUP_CATCHUP_NOTE
            return section

    # Compile times of the PR build and of the (older, different-machine)
    # master baselines carry a uniform machine-speed skew: with enough matched
    # TUs the whole table shifts by the same ratio. The median ratio estimates
    # that skew; individual TUs are flagged only when they deviate from it, and
    # the skew itself is judged separately on the section level (below): a
    # heavy common header slowing down every TU by the same factor cancels out
    # of every per-TU delta by construction, so it can only be caught there.
    # The true median (the average of the two middle ratios on an even count):
    # the upper middle element would overestimate the shift when a part of the
    # translation units regressed, normalizing that regression away.
    ratios = sorted(pr_durs[tu] / max(base_dur, 1) for tu, (base_dur, *_) in base_durs.items() if tu in pr_durs)
    skew = statistics.median(ratios) if len(ratios) >= 10 else 1.0

    # The aggregate cost of that shift, measured without applying the skew:
    # what the matched translation units really cost on each side.
    matched = [(pr_durs[tu], base_dur) for tu, (base_dur, *_) in base_durs.items() if tu in pr_durs]
    matched_delta_s = sum(pr_dur - base_dur for pr_dur, base_dur in matched) / 1e6

    total_s = sum(pr_durs.values()) / 1e6
    findings = []
    for tu, pr_dur in pr_durs.items():
        if tu not in base_durs:
            continue
        base_dur, base_tu_sha, base_cst, base_iid = base_durs[tu]
        base_tu_side = Side(date_condition, 0, base_tu_sha, base_cst, base_iid, WARMUP_CHECK_NAME)
        adjusted_base = base_dur * skew
        delta_s = (pr_dur - adjusted_base) / 1e6
        ratio = max(pr_dur, adjusted_base) / max(min(pr_dur, adjusted_base), 1)
        if abs(delta_s) >= TU_REPORT_SECONDS and ratio >= TU_REPORT_RATIO:
            findings.append((tu, pr_dur, base_dur, base_tu_side, delta_s, ratio))
    findings.sort(key=lambda f: -abs(f[4]))

    lines = [f"{len(pr_durs)} translation units recompiled, {total_s:.0f} s compile time in total, {len(base_durs)} of them have a recent master baseline."]
    if abs(skew - 1.0) >= 0.05:
        lines += [
            "",
            f"Median compile-time ratio to the baselines is ×{skew:.2f} (machine-speed difference or a change affecting every TU); per-TU deltas below are relative to that ratio.",
            f"The matched translation units cost {format_seconds_delta(matched_delta_s, sum(base for _, base in matched) / 1e6)} in total before that adjustment.",
        ]
    # A section-wide shift is invisible per TU: every delta is measured against
    # the median ratio, so a change that moves everything in the same direction
    # moves the ratio itself and leaves the deltas at zero. Judge it here
    # instead - symmetrically: a uniform speedup is as much a build-profile
    # change as a uniform slowdown, and normalizing it away would report a
    # large total compile-time drop as "no significant changes".
    skew_ratio = max(skew, 1.0 / skew) if skew > 0 else 1.0
    if skew_ratio >= TU_SKEW_SIG_RATIO and abs(matched_delta_s) >= TU_SKEW_SIG_SECONDS:
        section.significant = True
        direction = "slower" if matched_delta_s > 0 else "faster"
        section.summary = f"every translation unit is ×{skew_ratio:.2f} {direction} ({matched_delta_s:+.0f} s over {len(matched)} matched translation units)"
    if findings:
        lines += [
            "",
            "| Translation unit | Master | PR | Δ vs median |",
            "|---|---:|---:|---:|",
        ]
        for tu, pr_dur, base_dur, base_tu_side, delta_s, ratio in findings[:MAX_TABLE_ROWS]:
            if abs(delta_s) >= TU_SIG_SECONDS and ratio >= TU_SIG_RATIO:
                section.significant = True
            lines.append(f"| {md_code(tu_label(*tu))} | {base_dur / 1e6:.1f} s | {pr_dur / 1e6:.1f} s | {format_seconds_delta(delta_s, base_dur * skew / 1e6)} |")
        findings_summary = f"{len(findings)} translation units changed compile time"
        section.summary = f"{section.summary}; {findings_summary}" if section.summary else findings_summary

        # Attribute the biggest slowdowns to concrete frontend/backend entities
        # (template instantiations, included headers, function codegen).
        for tu, pr_dur, base_dur, base_tu_side, delta_s, ratio in findings[:3]:
            if delta_s <= 0:
                continue
            drill = drill_down_tu(db, pr_side, base_tu_side, *tu, skew=skew)
            if drill:
                lines += ["", f"Slowest changed entities of {md_code(tu_label(*tu))}:", ""]
                lines += drill

    # A brand-new TU has no baseline; its whole compile time is the change, so it
    # is judged by the same two thresholds as a both-sided delta: reported from
    # TU_REPORT_SECONDS, significant from TU_SIG_SECONDS. Reporting from a higher
    # threshold than the significance one used to hide 20-30 s new TUs from the
    # body, the summary and the verdict alike.
    new_tus = sorted(
        ((tu, d) for tu, d in pr_durs.items() if tu not in base_durs and d >= TU_REPORT_SECONDS * 1e6),
        key=lambda x: -x[1],
    )
    if new_tus:
        lines += ["", "Translation units without a recent master baseline:", ""]
        for tu, dur in new_tus[:MAX_TABLE_ROWS]:
            lines.append(f"- {md_code(tu_label(*tu))}: {dur / 1e6:.1f} s")
        if len(new_tus) > MAX_TABLE_ROWS:
            lines.append(f"- ... and {len(new_tus) - MAX_TABLE_ROWS} more")
        # A large new compile-time cost is significant even without a baseline to
        # diff against; a missing baseline must not silence the top-level verdict.
        big_new = [dur for _, dur in new_tus if dur >= TU_SIG_SECONDS * 1e6]
        if big_new:
            section.significant = True
            new_summary = f"{len(big_new)} new translation units without a master baseline"
            section.summary = f"{section.summary}; {new_summary}" if section.summary else new_summary

    section.body = "\n".join(lines)
    return section


def drill_down_tu(db: Db, pr_side, base_side, file, library, skew=1.0) -> List[str]:
    """Top per-entity compile time changes inside one translation unit.

    Entity deltas are filtered against the same median machine-speed ratio as
    the per-TU comparison, so a uniformly slower run does not list every
    entity of the TU.

    `base_side` is the exact run that produced this TU's baseline compile time
    (carried out of `compare_compile_times` so the drill-down reads its entities
    from the same run, not from a later rerun of the same commit that may not
    have recompiled this TU). The TU is identified by (file, library), like the
    per-TU comparison itself: a source compiled into several targets is several
    distinct compile jobs.
    """
    rows = db.query(
        f"""SELECT name, detail,
            sumIf(dur, side = 'pr') AS pr_dur,
            sumIf(dur, side = 'base') AS base_dur,
            toInt64(pr_dur) - toInt64(base_dur) AS delta
        FROM {
            both_sides(
                "build_time_trace",
                f"name, {strip_clone_suffix('detail')} AS detail, dur",
                pr_side,
                base_side,
                f"file = {quote(file)} AND library = {quote(library)} AND detail != '' AND name IN ('InstantiateFunction', 'InstantiateClass', 'ParseClass', 'Source', 'OptFunction', 'CodeGen Function')",
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
    sides have its symbol data; a binary whose symbols the master baseline has
    but the PR build did not upload means the profile producer lost rows, and
    is flagged as an incomplete comparison instead of an all-green omission.

    Symbols are keyed by the name with ThinLTO's clone suffix removed (see
    `strip_clone_suffix`): the suffix is not stable across builds, so without
    that the largest rows of the table are the same unchanged function listed
    twice, once removed and once new.
    """
    section = Section(title="Symbol sizes")
    sides = db.query(
        f"""SELECT file,
            countIf(side = 'pr') AS pr_count,
            countIf(side = 'base') AS base_count
        FROM {both_sides("binary_symbols", "file", pr_side, base_side, f"file IN ({in_list(FINAL_BINARIES)})")}
        GROUP BY file"""
    )
    comparable = [row["file"] for row in sides if int(row["pr_count"]) and int(row["base_count"])]
    missing_base = [row["file"] for row in sides if int(row["pr_count"]) and not int(row["base_count"])]
    missing_pr = [row["file"] for row in sides if int(row["base_count"]) and not int(row["pr_count"])]
    lines = []
    if missing_pr:
        names = ", ".join(md_code(strip_build_dir(f)) for f in missing_pr)
        lines.append(
            f"⚠️ The master baseline has symbol data for {names} but this build "
            "uploaded none: the profile producer lost it and the comparison is incomplete."
        )
        lines.append("")
        section.significant = True
        section.summary = f"missing PR-side symbol data for {', '.join(strip_build_dir(f) for f in missing_pr)}"
    if not comparable:
        if not missing_pr and any(int(row["pr_count"]) for row in sides):
            # The PR uploaded symbols but master has none at all: the baseline
            # simply predates symbol collection.
            lines.append("No symbol data for the master baseline yet (it is collected for release builds only since this check was introduced); the comparison will activate once master catches up.")
        section.body = "\n".join(lines).rstrip()
        return section
    # size >= 1 KiB cuts the aggregation to a fraction of the ~1.5M symbols of
    # the binary; a symbol can only reach the report threshold if one side
    # exceeds it anyway.
    rows = db.query(
        f"""SELECT file, symbol,
            sumIf(size, side = 'pr') AS pr_size,
            sumIf(size, side = 'base') AS base_size,
            toInt64(pr_size) - toInt64(base_size) AS delta
        FROM {
            both_sides(
                "binary_symbols",
                f"file, {strip_clone_suffix('symbol')} AS symbol, size",
                pr_side,
                base_side,
                f"file IN ({in_list(comparable)}) AND size >= 1024",
            )
        }
        GROUP BY file, symbol
        HAVING abs(delta) >= {SYMBOL_REPORT_BYTES}
        ORDER BY abs(delta) DESC
        LIMIT {MAX_TABLE_ROWS}"""
    )
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


def build_comment(info, pr_sha: str, base_sha: str, sections: List[Section], warmup_sha: Optional[str] = None) -> str:
    significant = [s for s in sections if s.significant]
    repo_url = f"https://github.com/{info.repo_name}"
    # `warmup_sha` names only the object-size baseline. Compile times are
    # resolved per translation unit - each against the most recent warmup
    # build that recompiled it - so they must not be attributed to any single
    # commit here (the two can legitimately differ, e.g. when the newest
    # warmup data is older than the object-size window).
    warmup_note = ""
    if warmup_sha and warmup_sha != base_sha:
        warmup_note = f"; object sizes against the warmup build of [`{warmup_sha[:9]}`]({repo_url}/commit/{warmup_sha})"
    lines = [
        f"### Build profile diff ({CHECK_NAME})",
        "",
        f"Comparing [`{pr_sha[:9]}`]({repo_url}/commit/{pr_sha}) with master "
        f"[`{base_sha[:9]}`]({repo_url}/commit/{base_sha}) "
        f"(stripped binary size, per-symbol sizes and ThinLTO time{warmup_note}; "
        "compile times per translation unit against the most recent warmup build that recompiled it).",
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


def update_comment(body: str, only_update: bool = False) -> None:
    """Post or update the tagged PR comment. A GH hiccup must not fail the check."""
    try:
        GH.post_updateable_comment(comment_tags_and_bodies={COMMENT_TAG: body}, only_update=only_update)
    except Exception:
        # The comparison result is still in the job report.
        print("WARNING: failed to post/update the PR comment")
        traceback.print_exc()


def run_comparison(db, info, args, pr_number: int, pr_sha: str):
    """Resolve both sides, compare every aspect and render the comment body."""
    master_shas = get_master_shas(info)
    if not master_shas:
        if not args.local:
            # Fail-close: in CI the anchored chain is the only baseline set. A
            # run that lost it must not fall back to the global master tip -
            # that would let the baseline be a commit the PR is not built on.
            raise RuntimeError("No `master_track_commits_sha` metadata - cannot anchor the baseline on the PR's master parent")
        # Local runs have no CI kv metadata: without a chain the warmup and
        # per-TU baseline lookups find nothing and `commit_sha IN ()` is not
        # even a valid query, so anchor the chain on the provided baseline.
        if not args.base_sha:
            raise RuntimeError("A local run has no CI master-chain metadata - pass --base-sha to anchor the baseline")
        master_shas = seed_master_shas(args.base_sha)
    # The baseline chain is frozen at the head's master parent, so the windows
    # that look for its profile rows are measured from the same event rather
    # than from the wall clock (see master_windows).
    windows = master_windows(info.event_time, {"base": BASE_DAYS, "tu": TU_BASE_DAYS})
    base_sha = args.base_sha or find_baseline(db, master_shas, pr_sha, windows["base"])
    if not base_sha:
        # Fail-close: no baseline means no comparison, not a comparison against
        # an arbitrary commit.
        raise RuntimeError("No master baseline with build profile data found - cannot compare")
    warmup_sha = find_warmup_baseline(db, master_shas, pr_sha, windows["base"])
    print(f"Comparing PR {pr_number} sha {pr_sha} against master {base_sha} (warmup baseline: {warmup_sha})")

    # Pin each side to one concrete build run once, and reuse it for every
    # table (see Side / resolve_run): the whole comparison then reflects a
    # single build instead of a per-table mix of reruns.
    pr_side = resolve_run(db, recent_days(PR_DAYS), pr_number, pr_sha)
    base_side = resolve_run(db, windows["base"], 0, base_sha)
    if pr_side is None or base_side is None:
        raise RuntimeError("Could not resolve a concrete build run for one of the sides")
    # The warmup baseline may lag while master catches up with profiling the
    # warmup build; the sections that depend on it degrade to a catch-up note.
    warmup_side = resolve_run(db, windows["base"], 0, warmup_sha, check_name=WARMUP_CHECK_NAME) if warmup_sha else None
    # The per-TU compile baseline looks back TU_BASE_DAYS - far past the ~100
    # commits of the anchored chain - so its candidate set is extended with
    # older ancestors (see extend_master_shas).
    tu_master_shas = extend_master_shas(master_shas, walk_cutoff(info.event_time, TU_BASE_DAYS))

    sections = [
        compare_binaries(db, pr_side, base_side),
        compare_objects(db, pr_side, warmup_side),
        compare_opt_functions(db, pr_side, base_side),
        compare_compile_times(db, pr_side, tu_master_shas, windows["tu"]),
        compare_symbols(db, pr_side, base_side),
    ]

    return build_comment(info, pr_sha, base_sha, sections, warmup_sha), sections, base_sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="local run: no GH comment, print to stdout")
    parser.add_argument("--pr-sha", help="override the PR-side commit sha")
    parser.add_argument("--pr-number", type=int, help="override the PR number")
    parser.add_argument("--base-sha", help="override the baseline master sha (required with --local)")
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
        # Replace any comparison posted for an older commit of this PR: leaving
        # it in place would show a stale revision as if it were the current one.
        # only_update: a PR that never built (a doc-only change reusing master's
        # build) has nothing to say and should not get a comment at all.
        update_comment(f"### Build profile diff ({CHECK_NAME})\n\n{info_text}.", only_update=True)
        Result.create_from(status=Result.Status.OK, info=info_text).complete_job()
        return

    try:
        body, sections, base_sha = run_comparison(db, info, args, pr_number, pr_sha)
    except Exception as e:
        # The tagged comment is pinned to the pull request, not to a commit, so
        # every exit path has to refresh it: any of the baseline lookups, run
        # resolutions or cluster reads can fail-close after an earlier commit
        # already posted a comparison, and leaving that one in place would
        # present a previous revision - possibly one the head reverted - as the
        # current comparison. only_update, as above: a pull request that never
        # got a comparison does not need one to say the job failed, the red
        # check says it.
        if not args.local:
            update_comment(
                f"### Build profile diff ({CHECK_NAME})\n\n"
                f"Comparing commit `{pr_sha}` with master failed: "
                f"{md_code(f'{type(e).__name__}: {e}'.replace(chr(10), ' '))}.\n\n"
                "See the job log for details.",
                only_update=True,
            )
        raise

    significant = [s for s in sections if s.significant]

    if args.local:
        print("=" * 80)
        print(body)
        return

    update_comment(body)

    summaries = [s.summary or s.title for s in significant]
    result_info = "Significant changes: " + "; ".join(summaries) if significant else f"No significant changes vs master {base_sha[:9]}"
    # Each comparison aspect is reported as its own sub-result: they land as
    # separate rows in cidb and read as separate lines in the CI report.
    sub_results = [
        Result(
            name=section.title,
            status=Result.Status.OK,
            info=section.summary or ("significant changes" if section.significant else ""),
        )
        for section in sections
    ]
    Result.create_from(status=Result.Status.OK, info=result_info, results=sub_results).complete_job()


if __name__ == "__main__":
    main()
