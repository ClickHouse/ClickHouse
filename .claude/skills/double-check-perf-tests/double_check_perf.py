#!/usr/bin/env python3
"""
Double-check ClickHouse CI performance comparison results locally.

Given a commit SHA from a PR that ran a perf comparison, this tool:
  1. Looks up the PR and the reference (left/baseline) SHA used by CI.
  2. Fetches the per-shard ``all-query-metrics.tsv`` files for the current
     machine's architecture and selects every row marked as a "Change in
     Performance" (``is_changed`` in compare.sh terminology).
  3. Downloads both the right (patched) and the left (reference) binaries
     from ``clickhouse-builds.s3.amazonaws.com``.
  4. Starts two local clickhouse-server processes (different ports, both
     reading from a hardlinked copy of the dataset directory, or each
     writing its own copy under --populate) configured exactly like CI's
     performance-comparison job.
  5. Reruns only the changed query indices via
     ``tests/performance/scripts/perf.py`` for each affected XML.
  6. Prints a comparison of local diffs against the original CI diffs.

The script does *not* re-implement compare.sh's full pipeline (we only
care about ``client_time`` for the queries that changed). It relies on
``perf.py`` for actually measuring and computing per-query diffs.

Run from the root of a ClickHouse checkout.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import itertools
import json
import math
import os
import platform
import random
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread
from typing import Optional

REPO = "ClickHouse/ClickHouse"

# Public S3 layout used by CI:
#   PRs:     clickhouse-builds.s3.amazonaws.com/PRs/<pr>/<sha>/pr/<build_type>/clickhouse
#   master:  clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/<sha>/masterci/<build_type>/clickhouse
BUILDS_BUCKET_PR = "https://clickhouse-builds.s3.amazonaws.com"
BUILDS_BUCKET_MASTER = "https://clickhouse-builds.s3.us-east-1.amazonaws.com"
REPORTS_BUCKET = "https://s3.amazonaws.com/clickhouse-test-reports"

# The S3 layout inserts the normalized workflow name as a path segment between
# <sha> and the per-job result/artifact directory (Utils.normalize_string of the
# workflow name; see get_s3_prefix_static in ci/praktika/_environment.py). The
# perf comparison and its PR builds run in the "PR" workflow; reference binaries
# are built by the "MasterCI" workflow.
PR_WORKFLOW_SEGMENT = "pr"
MASTER_WORKFLOW_SEGMENT = "masterci"

# Ports — must match performance_tests.py so config files we copy work.
LEFT_TCP = 9001
LEFT_KEEPER_TCP = 9181
LEFT_KEEPER_RAFT = 9234
LEFT_INTERSERVER = 9009
LEFT_HTTP = 8123

# The temporary preconfig server must not reuse LEFT_TCP: it starts before the
# measured servers, so nothing has claimed that port yet, and if something else
# is already listening there the readiness probe would succeed against *that*
# server and the RENAME would be issued against someone else's data.
PRECONFIG_TCP = 9101
PRECONFIG_KEEPER_TCP = 9281
PRECONFIG_KEEPER_RAFT = 9334
# The perf drop-in removes <http_port>, <mysql_port>, <postgresql_port> and
# <tcp_with_proxy_port>, and the command line overrides the TCP and Keeper
# ports -- but <interserver_http_port> survives from programs/server/config.xml
# and would otherwise stay on the default 9009 no matter what --port-offset
# says. A dev server owning 9009 then kills the preconfig server outright
# ("Listen [::]:9009 failed: Address already in use") before any rerun starts.
PRECONFIG_INTERSERVER = 9109

RIGHT_TCP = 19001
RIGHT_KEEPER_TCP = 19181
RIGHT_KEEPER_RAFT = 19234
RIGHT_INTERSERVER = 19009
RIGHT_HTTP = 18123

# These mirror `CHServer` in ci/jobs/performance_tests.py, which runs in a
# container with nothing else on the box. On a development machine the left
# side's values are simply the ClickHouse defaults, so any local server owns
# them and `ensure_ports_free` refuses the run -- correctly, since measuring
# against someone else's server would compare the wrong binaries. `--port-
# offset` shifts the whole set instead, which changes nothing about what is
# measured.
PORT_NAMES = (
    "LEFT_TCP", "LEFT_KEEPER_TCP", "LEFT_KEEPER_RAFT", "LEFT_INTERSERVER",
    "LEFT_HTTP", "PRECONFIG_TCP", "PRECONFIG_KEEPER_TCP",
    "PRECONFIG_KEEPER_RAFT", "PRECONFIG_INTERSERVER",
    "RIGHT_TCP", "RIGHT_KEEPER_TCP",
    "RIGHT_KEEPER_RAFT", "RIGHT_INTERSERVER", "RIGHT_HTTP",
)


def apply_port_offset(offset: int) -> None:
    for name in PORT_NAMES:
        shifted = globals()[name] + offset
        if not 1024 < shifted < 65536:
            die(f"--port-offset {offset} puts {name} at {shifted}, "
                "outside the usable port range")
        globals()[name] = shifted


def log(msg: str) -> None:
    print(f"[double-check] {msg}", flush=True)


def die(msg: str, code: int = 1) -> None:
    print(f"[double-check] ERROR: {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def http_get(url: str, timeout: int = 60) -> str:
    """Fetch URL, transparently decompress if the URL ends with ``.zst``,
    and decode. Falls back to utf-8 with replacement because the perf TSVs
    sometimes contain bytes that aren't valid UTF-8 (query display names
    with stray non-ASCII chars).
    """
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} for {url}: {e.reason}") from e
    if url.endswith(".zst"):
        raw = zstd_decompress(raw)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def zstd_decompress(data: bytes) -> bytes:
    """Decompress zstd-compressed bytes. Tries the ``zstandard`` Python
    package first (no subprocess), falls back to piping through ``zstd``
    (which is installed on every ClickHouse dev box)."""
    try:
        import zstandard  # type: ignore
        return zstandard.ZstdDecompressor().decompress(data)
    except ImportError:
        pass
    proc = subprocess.run(
        ["zstd", "-dcq", "-"], input=data, capture_output=True, check=False
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"zstd decompression failed: {proc.stderr.decode('utf-8', 'replace')[:200]}"
        )
    return proc.stdout


def http_head_ok(url: str) -> bool:
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status == 200
    except urllib.error.HTTPError:
        return False
    except Exception:
        return False


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    log(f"downloading {url}")
    subprocess.run(
        ["curl", "-sfL", "--retry", "3", "-o", str(tmp), url], check=True
    )
    tmp.rename(dest)


# ---------------------------------------------------------------------------
# Architecture detection
# ---------------------------------------------------------------------------


def detect_arch() -> tuple[str, str]:
    """Return (perf_arch, build_type) — one of ('amd', 'build_amd_release') or
    ('arm', 'build_arm_release'). perf_arch matches CI's PR-report arch tag."""
    m = platform.machine().lower()
    if m in ("x86_64", "amd64"):
        return ("amd", "build_amd_release")
    if m in ("aarch64", "arm64"):
        return ("arm", "build_arm_release")
    die(f"unsupported architecture: {m}")
    return ("", "")  # unreachable


# ---------------------------------------------------------------------------
# Commit -> PR / SHA resolution
# ---------------------------------------------------------------------------


def gh_api(args: list[str], what: str) -> str:
    """Run `gh api` with GH_CONFIG_DIR dropped.

    Some agent and CI runners point GH_CONFIG_DIR at a config dir with no
    usable auth, which makes every gh call fail with HTTP 403 while the
    default config authenticates fine. Other tooling in this repo already
    works around it the same way (.claude/tools/gh-ro.sh,
    patch-release-check)."""
    env = {k: v for k, v in os.environ.items() if k != "GH_CONFIG_DIR"}
    try:
        return subprocess.check_output(["gh", "api", *args], text=True, env=env)
    except FileNotFoundError:
        die(f"gh not found on PATH, needed to {what}")
    except subprocess.CalledProcessError as e:
        die(f"gh failed to {what}: {e}")


def find_pr_for_commit(sha: str) -> int:
    """Use gh to find the PR number that contains this commit."""
    out = gh_api(
        [
            f"repos/{REPO}/commits/{sha}/pulls",
            "--jq",
            ".[] | {number: .number, state: .state}",
        ],
        f"resolve PR for commit {sha}",
    )
    prs = [json.loads(line) for line in out.strip().splitlines() if line.strip()]
    if not prs:
        die(f"no PR found containing commit {sha}")
    # Prefer the most recent open PR, otherwise just the highest number
    prs.sort(key=lambda p: (p["state"] != "open", -p["number"]))
    return prs[0]["number"]


def find_full_sha(sha: str) -> str:
    """Resolve possibly-short SHA against GitHub for the canonical 40-char SHA."""
    if len(sha) == 40 and all(c in "0123456789abcdef" for c in sha.lower()):
        return sha.lower()
    return gh_api(
        [f"repos/{REPO}/commits/{sha}", "--jq", ".sha"],
        f"resolve SHA {sha}",
    ).strip()


# ---------------------------------------------------------------------------
# Performance shard discovery (adapted from .claude/tools/fetch_perf_report.py)
# ---------------------------------------------------------------------------


@dataclass
class PerfShard:
    name: str
    arch: str
    baseline: str  # "master_head" or "release_base"
    shard_num: int
    total_shards: int
    tsv_url: str
    base_dir_url: str  # URL prefix where report.html, all-queries.html live
    status: str


def normalize_job_name(name: str) -> str:
    result = name.lower()
    result = re.sub(r"[^a-z0-9]", "_", result)
    result = re.sub(r"_+", "_", result)
    return result.rstrip("_")


# praktika statuses (ci/praktika/result.py) that mean the shard published no
# artifacts. Everything else -- including FAIL and ERROR -- normally still
# uploads a report, so those shards are kept and simply read.
NOT_RUN_STATUSES = {"SKIPPED", "PENDING", "RUNNING", "DROPPED"}

# The only baseline this tool can reproduce. CI runs a second flavour of the
# comparison, `release_base`, which measures against the latest release build
# and checks out that release's `tests/performance` (see the
# `compare_against_release` branch of ci/jobs/performance_tests.py). Nothing
# below is baseline-aware: the left binary is fetched from REFs/master/<sha>,
# query indices are positional in the tests tree of the commit under test, and
# the reference-SHA lookup cannot discriminate either, because the
# `query_metrics_v2` table exposed on play.clickhouse.com carries no
# `baseline_kind` column to filter on. Reports carrying such a shard are
# refused rather than half-answered.
SUPPORTED_BASELINE = "master_head"


def get_performance_shards(pr_number: int, sha: str) -> list[PerfShard]:
    pr_json_url = f"{REPORTS_BUCKET}/PRs/{pr_number}/{sha}/{PR_WORKFLOW_SEGMENT}/result_pr.json"
    pr_json = json.loads(http_get(pr_json_url))
    shards: list[PerfShard] = []

    def walk(results) -> None:
        if not results:
            return
        for r in results:
            name = r.get("name", "")
            if name.startswith("Performance Comparison"):
                m = re.match(
                    r"Performance Comparison\s*\((\w+)_release,\s*(\w+),\s*(\d+)/(\d+)\)",
                    name,
                )
                if m:
                    arch = m.group(1)
                    baseline = m.group(2)
                    shard_num = int(m.group(3))
                    total_shards = int(m.group(4))

                    dir_name = normalize_job_name(name)
                    base_dir = f"{REPORTS_BUCKET}/PRs/{pr_number}/{sha}/{PR_WORKFLOW_SEGMENT}/{dir_name}"

                    tsv_link = None
                    for link in r.get("links", []) or []:
                        if isinstance(link, str) and "all-query-metrics.tsv" in link:
                            tsv_link = link
                            break
                    if not tsv_link:
                        tsv_link = f"{base_dir}/all-query-metrics.tsv"

                    shards.append(
                        PerfShard(
                            name=name,
                            arch=arch,
                            baseline=baseline,
                            shard_num=shard_num,
                            total_shards=total_shards,
                            tsv_url=tsv_link,
                            base_dir_url=base_dir,
                            status=r.get("status", "unknown"),
                        )
                    )
            if r.get("results"):
                walk(r["results"])

    walk(pr_json.get("results"))
    return shards


# ---------------------------------------------------------------------------
# Changed query discovery
# ---------------------------------------------------------------------------


@dataclass
class ChangedQuery:
    test: str
    query_index: int
    arch: str           # which arch CI flagged this on
    shard_num: int
    left: float
    right: float
    diff: float
    stat_threshold: float
    direction: str
    query_display_name: str
    # Per-query bar CI used to flag this query; None on shards predating the
    # column, where CHANGED_THRESHOLD_FLOOR applies.
    changed_threshold: Optional[float] = None
    # CI flagged this query and then demoted it in its own confirmation rerun.
    ci_unconfirmed: bool = False
    # The numbers came from report.html because the TSV had no row for this
    # query. Only such a row is missing a changed_threshold CI *did* export;
    # an old shard predating the column is a different case, and keeps the
    # floor.
    numbers_from_html: bool = False
    # Set when the per-query threshold CI used could not be recovered. Such a
    # query gets no verdict: judging it by the bare floor would apply a weaker
    # gate than CI's and could call a noisy query CONFIRMED.
    threshold_unknown: bool = False
    # Filled in later, after we've collected all changes across arches:
    # which arches CI flagged this same (test, query_index) on. Useful so
    # the report can say "flagged on ARM only" vs "flagged on both".
    flagged_on: list[str] = field(default_factory=list)
    # Every arch's CI numbers for this (test, query_index). The dedup below
    # keeps a single row per query, so without this the second arch's numbers
    # and direction are lost -- and CI can legitimately call the same query
    # slower on one arch and faster on the other, which is a finding in itself.
    ci_by_arch: dict[str, dict] = field(default_factory=dict)


def parse_query_metrics_tsv(text: str, metric: str = "client_time"):
    """Yield rows from all-query-metrics.tsv.

    Layout (compare.sh report()):
        metric_name, left, right, diff, times_change, stat_threshold,
        test, query_index, query_display_name,
        changed_threshold, unstable_threshold

    The two trailing threshold columns are the per-query thresholds compare.sh
    computes in `report_thresholds` (the 0.15/0.25 floors raised by the
    historical p99 and the per-test `<max_ignored_relative_change>`), exported
    so consumers can classify with the same effective bar as the CI gate
    instead of a floor constant. They are appended at the end, so older shards
    that predate them simply have fewer columns.
    """
    for line in text.splitlines():
        if not line:
            continue
        cols = line.split("\t")
        if len(cols) < 9:
            continue
        if cols[0] != metric:
            continue
        try:
            left_v = float(cols[1])
            right_v = float(cols[2])
            diff_v = float(cols[3])
            stat_thr = float(cols[5])
            qi = int(cols[7])
        except ValueError:
            continue
        try:
            changed_thr = float(cols[9]) if len(cols) > 9 else None
        except ValueError:
            changed_thr = None
        yield {
            "metric": cols[0],
            "left": left_v,
            "right": right_v,
            "diff": diff_v,
            "times_change": cols[4],
            "stat_threshold": stat_thr,
            "test": cols[6],
            "query_index": qi,
            "query_display_name": cols[8] if len(cols) > 8 else "",
            "changed_threshold": changed_thr,
        }


def parse_report_table(html: str, table_id: str) -> set[tuple[str, int]]:
    """Extract (test, query_index) for every row of one report table. The HTML
    wraps each row with an id like ``<table_id>.<test>.<query_index>``."""
    m = re.search(rf"id={re.escape(table_id)}.*?</table>", html, re.DOTALL)
    if not m:
        return set()
    out: set[tuple[str, int]] = set()
    for test, qi in re.findall(
        rf"<tr id={re.escape(table_id)}\.([^>.]+?)\.(\d+)>", m.group(0)
    ):
        out.add((test, int(qi)))
    return out


def parse_changes_in_performance(html: str) -> set[tuple[str, int]]:
    return parse_report_table(html, "changes-in-performance")


def parse_changed_rows_from_html(html: str) -> dict[tuple[str, int], dict]:
    """Timings for the "Changes in Performance" rows, read out of the report.

    compare.sh retracts a query from ``all-query-metrics.tsv`` entirely when
    its confirmation rerun demoted it, while ``report.html`` still lists it
    under both "Changes in Performance" and "Unconfirmed Changes". Those rows
    carry their own numbers, so they do not have to be dropped for want of a
    TSV row. Columns, per the table header: old, new, ratio, diff,
    stat_threshold, test, index, query."""
    m = re.search(r"id=changes-in-performance.*?</table>", html, re.DOTALL)
    if not m:
        return {}
    rows: dict[tuple[str, int], dict] = {}
    for row_m in re.finditer(
        r"<tr id=changes-in-performance\.([^>.]+?)\.(\d+)>(.*?)</tr>",
        m.group(0),
        re.DOTALL,
    ):
        test, qi = row_m.group(1), int(row_m.group(2))
        # report.py writes cell values into <td> raw -- it does not import
        # `html`, let alone escape -- so a query containing `<` and `>` puts
        # them straight into the markup. Stripping tags inside a cell would eat
        # the text between them ("a < b AND c > d" becomes "a  d"), so only the
        # numeric cells, which never contain markup, are stripped; the query
        # text is taken verbatim.
        raw_cells = re.findall(r"<td[^>]*>(.*?)</td>", row_m.group(3), re.DOTALL)
        cells = [re.sub("<[^>]+>", "", c).strip() for c in raw_cells]
        if len(cells) < 8:
            continue
        try:
            rows[(test, qi)] = {
                "left": float(cells[0]),
                "right": float(cells[1]),
                "diff": float(cells[3]),
                "stat_threshold": float(cells[4]),
                "query_display_name": raw_cells[7].strip(),
                "changed_threshold": None,
            }
        except ValueError:
            continue
    return rows


def describe_unreadable(unreadable: list[tuple[str, int, int, str]]) -> str:
    return ", ".join(f"{a} {n}/{t}" for a, n, t, _ in sorted(unreadable))


def find_changed_queries(
    shards: list[PerfShard],
) -> tuple[list[ChangedQuery], int, list[tuple[str, int, str, int]],
           list[tuple[str, int, int, str]]]:
    """Identify rows the CI report flags under "Changes in Performance".

    compare.sh computes the predicate at report time using per-test thresholds
    (default ``changed_threshold=0.1`` raised by historical 99th-percentile
    diff and the ``<report_threshold>`` in the XML). We don't have those
    inputs outside CI, so instead of re-implementing the predicate we let
    CI tell us what counts: fetch each shard's ``report.html``, extract the
    ``id=changes-in-performance.<test>.<query_index>`` table, then pull the
    timing numbers for those tuples from ``all-query-metrics.tsv``. This is
    exactly the set the user sees in the report.

    Also returns how many shard reports were actually readable, the shards
    that could not be read at all, and any row CI flagged whose numbers could
    be read from neither source -- none of those may be silently dropped, or a
    non-empty CI report turns into an all-clear: a shard whose report cannot be
    fetched contributes no changed queries, exactly like a shard that had none,
    and the caller must not read the second as the first.
    """
    changed: list[ChangedQuery] = []
    read_ok = 0
    unresolved: list[tuple[str, int, str, int]] = []
    unreadable: list[tuple[str, int, int, str]] = []
    for s in shards:
        try:
            html = http_get(f"{s.base_dir_url}/report.html")
        except RuntimeError as e:
            log(f"skipping shard {s.arch}/{s.shard_num} report.html: {e}")
            unreadable.append((s.arch, s.shard_num, s.total_shards, str(e)))
            continue
        read_ok += 1
        flagged = parse_changes_in_performance(html)
        if not flagged:
            continue
        html_rows = parse_changed_rows_from_html(html)
        demoted = parse_report_table(html, "unconfirmed-changes")

        try:
            tsv = http_get(s.tsv_url)
        except RuntimeError as e:
            log(f"shard {s.arch}/{s.shard_num} TSV unreadable ({e}); "
                "using the numbers in report.html")
            tsv = ""
        # Index TSV rows so we can fetch timing numbers for each flagged
        # (test, query_index) tuple. There's one row per metric, we only
        # want client_time.
        timings: dict[tuple[str, int], dict] = {}
        for row in parse_query_metrics_tsv(tsv):
            timings[(row["test"], row["query_index"])] = row

        for test, qi in sorted(flagged):
            row = timings.get((test, qi))
            unconfirmed = False
            from_html = row is None
            if row is None:
                # compare.sh retracts a demoted query from the TSV. Its numbers
                # are still in the report, and it is precisely the kind of
                # ambiguous result a local rerun should adjudicate, so take
                # them rather than dropping the query.
                row = html_rows.get((test, qi))
                unconfirmed = (test, qi) in demoted
                if row is None:
                    log(
                        f"  WARNING: report.html flagged {test} #{qi} but "
                        f"neither the TSV nor the report row could be read in "
                        f"shard {s.arch}/{s.shard_num}"
                    )
                    unresolved.append((s.arch, s.shard_num, test, qi))
                    continue
                log(
                    f"  {test} #{qi}: no TSV row"
                    + (" (CI demoted it in its confirmation rerun)" if unconfirmed else "")
                    + " — using the numbers from report.html"
                )
            direction = "slower" if row["diff"] > 0 else "faster"
            changed.append(
                ChangedQuery(
                    test=test,
                    query_index=qi,
                    arch=s.arch,
                    shard_num=s.shard_num,
                    left=row["left"],
                    right=row["right"],
                    diff=row["diff"],
                    stat_threshold=row["stat_threshold"],
                    direction=direction,
                    query_display_name=row["query_display_name"],
                    changed_threshold=row.get("changed_threshold"),
                    ci_unconfirmed=unconfirmed,
                    numbers_from_html=from_html,
                )
            )
    return changed, read_ok, unresolved, unreadable


# ---------------------------------------------------------------------------
# External-dataset detection
# ---------------------------------------------------------------------------

# Tables that come from the canonical perf-test tarballs at
# clickhouse-datasets.s3.amazonaws.com. Anything the test creates inline via
# <create_query> doesn't show up here. We probe for these names as whole
# words inside the XML; any other reference (e.g. numbers_mt, generateRandom)
# is assumed to be self-contained.
EXTERNAL_DATASETS = {
    "hits_10m_single": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_10m_single.tar",
    "hits_100m_single": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_100m_single.tar",
    "hits_v1": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
    "hits": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
    "test.hits": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
    "test_values": "https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar",
    "tpch.lineitem": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.customer": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.orders": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.part": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.partsupp": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.supplier": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.nation": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpch.region": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar",
    "tpcds.store_sales": "https://clickhouse-datasets.s3.amazonaws.com/ds/scale_1/tpcds.tar",
    "tpcds.catalog_sales": "https://clickhouse-datasets.s3.amazonaws.com/ds/scale_1/tpcds.tar",
    "tpcds.web_sales": "https://clickhouse-datasets.s3.amazonaws.com/ds/scale_1/tpcds.tar",
    "tpcds.item": "https://clickhouse-datasets.s3.amazonaws.com/ds/scale_1/tpcds.tar",
}


UPSTREAM_URL = f"https://github.com/{REPO}.git"


def fetch_commit(repo: Path, sha: str, refs: list[str], remotes: tuple[str, ...],
                 depth: int = 0) -> bool:
    """Fetch `sha` into `repo`, trying each remote and ref. True once it is
    there.

    Success is decided by the commit being present afterwards, never by the
    fetch's exit code, so a ref that resolves to something else on the wrong
    remote -- `refs/pull/<n>/head` of a fork is a different pull request
    entirely -- cannot be mistaken for the commit we asked for."""
    for remote in remotes:
        for ref in refs:
            cmd = ["git", "-C", str(repo), "fetch", "--quiet"]
            if depth:
                cmd += [f"--depth={depth}"]
            cmd += [remote, ref]
            subprocess.run(cmd, capture_output=True)
            if subprocess.run(
                ["git", "-C", str(repo), "cat-file", "-e", f"{sha}^{{commit}}"],
                capture_output=True,
            ).returncode == 0:
                return True
    return False


def init_scratch_repo(repo_root: Path, scratch: Path) -> bool:
    """An empty repository under the work dir pointed at the same origin, for
    when the real clone's .git cannot be written to."""
    url = subprocess.run(
        ["git", "-C", str(repo_root), "remote", "get-url", "origin"],
        capture_output=True, text=True,
    )
    # The scratch repo's "origin" is the local one when there is one, but
    # fetch_commit falls back to the canonical upstream, so a fork checkout --
    # or no origin at all -- still resolves the commit.
    origin = url.stdout.strip() if url.returncode == 0 else UPSTREAM_URL
    scratch.mkdir(parents=True, exist_ok=True)
    if not (scratch / ".git").is_dir():
        if subprocess.run(
            ["git", "init", "--quiet", str(scratch)], capture_output=True
        ).returncode != 0:
            return False
        if subprocess.run(
            ["git", "-C", str(scratch), "remote", "add", "origin", origin],
            capture_output=True,
        ).returncode != 0:
            return False
    return True


def materialize_perf_tree(repo_root: Path, pr_number: int, sha: str,
                          work_dir: Path) -> Path:
    """Extract the test tree of the commit under test into the work dir.

    The commit decides which report and binaries we fetch, but the queries, the
    XMLs, perf.py and the perf config drop-ins were being taken from whatever
    the working tree happens to hold. Query numbering is positional and
    substitutions expand it, so an XML that gained or lost a query since --
    or a `refs/pull/<n>/merge` checkout, which is not the commit CI measured --
    silently shifts every index, and the rerun then validates a different query
    than the one CI flagged, judged by a different perf.py. CI has no such gap:
    its checkout *is* the commit. Pin the same way."""
    dst = work_dir / "perf-tree" / sha[:12]
    marker = dst / ".extracted"
    if marker.is_file():
        return dst
    have = subprocess.run(
        ["git", "-C", str(repo_root), "cat-file", "-e", f"{sha}^{{commit}}"],
        capture_output=True,
    )
    archive_from = repo_root
    if have.returncode != 0:
        log(f"commit {sha[:12]} not in the local clone, fetching it")
        refs = [f"refs/pull/{pr_number}/head", sha]
        # Only this checkout's own origin, and only here: fetching upstream
        # into someone's clone is unbounded work when the history is not
        # already there, and shallow-fetching into a full clone would leave a
        # .git/shallow behind. The scratch repo below does that part, cheaply
        # and where nothing else cares.
        if not fetch_commit(repo_root, sha, refs, remotes=("origin",)):
            # Fetching writes to .git (FETCH_HEAD at minimum), which some
            # sandboxes mount read-only. A scratch repository under the work
            # dir needs nothing from .git but the remote URL, and produces the
            # same commit.
            log("fetching into the clone failed; using a scratch repository "
                "under the work dir")
            scratch = work_dir / "perf-tree" / ".fetch"
            if not init_scratch_repo(repo_root, scratch):
                die(
                    f"commit {sha} is not available locally, could not be "
                    "fetched into this clone, and a scratch repository could "
                    "not be created. Fetch the commit yourself, or pass "
                    "--use-working-tree-tests to accept this checkout's tests."
                )
            # A fork checkout's origin has a different refs/pull/<n>/head, or
            # none, so the canonical upstream is tried as well.
            if not fetch_commit(
                scratch, sha, refs, remotes=("origin", UPSTREAM_URL), depth=1
            ):
                die(
                    f"commit {sha} could not be fetched (tried "
                    f"refs/pull/{pr_number}/head and the SHA itself, from this "
                    f"checkout's origin and from {UPSTREAM_URL}). Fetch it "
                    "yourself, or pass --use-working-tree-tests."
                )
            archive_from = scratch
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)
    # Everything the rerun reads from the repo: the XMLs and perf.py, the perf
    # config drop-ins, the base server config, and the TLD files.
    paths = [
        "tests/performance",
        # tpch.xml, tpcds.xml and the tpch-join_algorithm-* tests read their
        # SQL bodies and settings from here via <query file="..."/> and
        # <settings file="..."/>, so the extracted tree is unusable for them
        # without it.
        "tests/benchmarks",
        "programs/server",
        # Not just top_level_domains: most of programs/server/config.d and
        # users.d are symlinks into tests/config (`clusters.xml`,
        # `keeper_port.xml`, `ext-en.txt`, ...), and prepare_configs copies
        # them dereferenced, the way the CI job does with
        # `cp -r --dereference`. Extracting only the TLD files leaves those
        # links dangling and the copy fails outright. The whole directory is
        # ~1 MB.
        "tests/config",
    ]
    archive = subprocess.run(
        ["git", "-C", str(archive_from), "archive", sha, *paths],
        capture_output=True,
    )
    if archive.returncode != 0:
        die(
            f"git archive failed for {sha[:12]}: "
            f"{archive.stderr.decode(errors='replace').strip()}"
        )
    extract = subprocess.run(
        ["tar", "-x", "-C", str(dst)], input=archive.stdout, capture_output=True
    )
    if extract.returncode != 0:
        die(f"could not extract the test tree: "
            f"{extract.stderr.decode(errors='replace').strip()}")
    marker.write_text(sha + "\n")
    log(f"using tests/performance and configs from {sha[:12]} ({dst})")
    return dst


def selected_query_text(repo_root: Path, xml_path: Path,
                        query_indices: list[int]) -> str:
    """The bodies of exactly the given query indices, expanded.

    Asks perf.py, which owns the numbering: one `<query>` element with
    substitutions expands into several numbered queries, and `<query
    file="...">` bodies are inlined. `--print-queries` exits before connecting
    to any server, so this is cheap and needs nothing running."""
    cmd = [
        sys.executable,
        "-c", PRINT_QUERIES_RUNNER,
        str(repo_root / "tests/performance/scripts/perf.py"),
        str(xml_path),
        "--print-queries",
        "--queries-to-run", *[str(i) for i in query_indices],
    ]
    try:
        return subprocess.check_output(cmd, text=True, timeout=120,
                                       stderr=subprocess.DEVNULL)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        die(f"perf.py --print-queries failed for {xml_path.name}: {e}")


# perf.py imports clickhouse_driver and scipy at module scope, before the
# --print-queries early exit, so listing queries needs packages that only the
# benchmark itself uses. Rather than reimplement the expansion (substitutions
# turn one <query> into several, and `file=` bodies are inlined), run perf.py
# with stand-ins for whatever is missing: the metadata path never touches them,
# and using perf.py's own code keeps the numbering identical to CI's.
PRINT_QUERIES_RUNNER = """\
import runpy, sys, types

def _stub(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module

try:
    import clickhouse_driver  # noqa: F401
except ImportError:
    driver = _stub("clickhouse_driver")
    errors = _stub("clickhouse_driver.errors")
    class _Error(Exception):
        pass
    errors.Error = _Error
    driver.errors = errors
    sys.modules["clickhouse_driver"] = driver
    sys.modules["clickhouse_driver.errors"] = errors

try:
    from scipy import stats  # noqa: F401
except ImportError:
    scipy = _stub("scipy")
    scipy_stats = _stub("scipy.stats")
    scipy.stats = scipy_stats
    sys.modules["scipy"] = scipy
    sys.modules["scipy.stats"] = scipy_stats

sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")
"""


def require_perf_dependencies() -> None:
    """Fail before the downloads when the packages the rerun itself needs are
    missing. Only the metadata path can do without them."""
    missing = []
    for module in ("clickhouse_driver", "scipy"):
        if importlib.util.find_spec(module) is None:
            missing.append(module)
    if missing:
        die(
            "perf.py needs " + " and ".join(missing) + " to run the benchmark; "
            "install " + " ".join(missing) + " (a virtualenv is fine) and retry. "
            "--dry-run works without them."
        )


def query_display_name_from_tree(repo_root: Path, xml_path: Path,
                                index: int) -> str:
    """The `query_display_name` CI recorded for this query, derived from the
    test tree rather than scraped from the report.

    perf.py builds it as `query_display(item)` trimmed to 1000 characters, and
    that is what it prints for `--print-queries` and what ends up in
    `query_metrics_v2`. Taking it from the tree avoids the report's unescaped
    markup entirely, which matters because this name is half the key of the
    historical-threshold lookup."""
    name = selected_query_text(repo_root, xml_path, [index])
    # perf.py emits its `stage` progress lines before the queries. Drop those,
    # then the single trailing newline `print` added -- and nothing else: the
    # recorded name keeps the query's own leading and trailing whitespace.
    while name.startswith("stage\t"):
        name = name.split("\n", 1)[1] if "\n" in name else ""
    if name.endswith("\n"):
        name = name[:-1]
    if len(name) > 1000:
        name = f"{name[:1000]}...({index})"
    return name


def relevant_test_text(repo_root: Path, xml_path: Path,
                       query_indices: list[int]) -> str:
    """Text deciding which datasets a rerun of *these* queries needs.

    Scanning the whole XML over-reports: `aggregation_in_order.xml` reads
    hits_10m_single and hits_100m_single from different queries, so rerunning
    one of them would demand both tarballs. Only the selected queries count --
    plus every `create_query`/`fill_query`/`drop_query`, which perf.py runs
    whatever `--queries-to-run` says, and which it expands over *all*
    substitution combinations."""
    parts: list[str] = []
    try:
        root = ET.fromstring(xml_path.read_text())
    except (OSError, ET.ParseError):
        root = None
    if root is not None:
        prep = "\n".join(
            q.text or ""
            for q in root
            if q.tag in ("create_query", "fill_query", "drop_query")
        )
        parts.append(prep)
        # A preparation query with a substitution placeholder runs once per
        # value, and which value lands where is not decidable from the text --
        # so every value counts. `<fill_query>USE {database}</fill_query>` in
        # tpch.xml is exactly this case.
        if "{" in prep:
            parts.extend(
                v.text or ""
                for v in root.findall("substitutions/substitution/values/value")
            )
    parts.append(selected_query_text(repo_root, xml_path, query_indices))
    return "\n".join(parts)


def scan_external_datasets(texts: dict[str, str]) -> dict[str, list[str]]:
    """Return {dataset_name: [test, ...]} for every external dataset referenced
    by the given per-test text (see relevant_test_text)."""
    found: dict[str, list[str]] = defaultdict(list)
    db_aliases = {
        "tpcds": "tpcds.store_sales",   # any tpcds.* entry maps to the same tarball
        "tpch":  "tpch.lineitem",
        "tpch10": "tpch.lineitem",      # the value tpch.xml substitutes
    }
    for test_name, text in texts.items():
        for name in EXTERNAL_DATASETS:
            if re.search(rf"(?<![\w.]){re.escape(name)}(?![\w])", text):
                found[name].append(test_name)

        # Heuristic for benchmarks whose tables aren't database-qualified in
        # the SQL bodies. Catch both forms:
        #   - direct: "USE tpcds" / "FROM tpcds" / etc.
        #   - via substitution: "USE {table}" with <value>tpcds</value> in
        #     a substitution block (this is how tpcds.xml / tpch.xml work).
        for db_word, canonical in db_aliases.items():
            if re.search(rf"\b(?:USE|FROM)\s+{db_word}\b", text, re.IGNORECASE):
                found[canonical].append(test_name)
                continue
            if re.search(rf"(?<![\w.]){db_word}(?![\w])", text):
                found[canonical].append(test_name)
    return dict(found)


# ---------------------------------------------------------------------------
# Reference SHA discovery (the "left" binary)
# ---------------------------------------------------------------------------


# The same query ci/jobs/performance_tests.py sends to CIDB before a perf run,
# with `today()` left as a parameter: the window is anchored on the day the run
# happened, not on today, so the numbers are the ones that run actually used.
#
# JSONEachRow, not TSV, because `query_display_name` is half the lookup key and
# most of them are multi-line: `query_display` joins statements with ";\n" and
# keeps the XML body's own newlines and indentation. TSV output re-escapes
# those to a literal backslash-n, while the other side of the join derives the
# name from the test tree and holds the real characters, so every multi-line
# query would miss its historical row and fall back to the bare 0.15 floor --
# a weaker gate than the one CI used, which is how a noisy demoted query turns
# into a false CONFIRMED. JSON round-trips the raw string exactly.
HISTORICAL_THRESHOLDS_QUERY = """\
SELECT test, query_index, quantileExact(0.99)(abs(diff)) * 1.5 AS max_diff,
    any(query_display_name) AS query_display_name
FROM query_metrics_v2
WHERE event_date BETWEEN toDate('{day}') - INTERVAL 1 MONTH - INTERVAL 1 WEEK
        AND toDate('{day}') - INTERVAL 1 WEEK
    AND metric = 'client_time'
    AND pr_number = 0
GROUP BY test, query_index
HAVING count() > 100
FORMAT JSONEachRow"""


def play_query(query: str, what: str) -> Optional[str]:
    """Run a query against play.clickhouse.com. Returns None on failure -- the
    caller decides what an unavailable answer means."""
    try:
        return subprocess.check_output(
            ["clickhouse", "client", "--host", "play.clickhouse.com",
             "--user", "explorer", "--secure", "--query", query],
            text=True, timeout=120, stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        log(f"clickhouse client not on PATH, cannot {what}")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        err = getattr(e, "stderr", b"") or b""
        if isinstance(err, bytes):
            err = err.decode(errors="replace")
        log(f"play.clickhouse.com query failed ({what}): {str(err).strip()[:200]}")
    return None


def fetch_historical_thresholds(
    run_day: str,
) -> Optional[dict[tuple[str, int, str], float]]:
    """The `max_diff` per query that CI's own threshold computation used.

    Only needed for queries CI demoted: compare.sh retracts those from the TSV,
    taking the exported `changed_threshold` with them, and judging them by the
    bare 0.15 floor instead would apply a weaker gate than the one CI used to
    flag them in the first place.

    Keyed by (test, query_index, query_display_name) because that is the join
    compare.sh performs. The index alone is not an identity: it is positional,
    so an edited query body at the same index would otherwise inherit the
    learned threshold of the query that used to be there. Under CI's join such
    a query simply finds no historical row and falls back to the floor, and
    keying the same way reproduces that."""
    out = play_query(
        HISTORICAL_THRESHOLDS_QUERY.format(day=run_day), "fetch historical thresholds"
    )
    if out is None:
        return None
    thresholds: dict[tuple[str, int, str], float] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            key = (row["test"], int(row["query_index"]),
                   row["query_display_name"])
            thresholds[key] = float(row["max_diff"])
        except (ValueError, TypeError, KeyError):
            continue
    return thresholds


def test_report_threshold(xml_path: Path) -> float:
    """The test's own `max_ignored_relative_change`, the third input to
    compare.sh's per-query threshold."""
    try:
        root = ET.fromstring(xml_path.read_text())
    except (OSError, ET.ParseError):
        return 0.0
    try:
        return float(root.attrib.get("max_ignored_relative_change", 0.0))
    except ValueError:
        return 0.0


def ci_changed_threshold(historical: float, per_test: float) -> float:
    """compare.sh: ceil(greatest(0.15, historical_max_diff, report_threshold), 2)."""
    return math.ceil(max(CHANGED_THRESHOLD_FLOOR, historical, per_test) * 100) / 100


def fetch_run_day(pr_number: int, pr_sha: str, perf_arch: str) -> Optional[str]:
    """The date CI ran this comparison, which anchors the historical window."""
    out = play_query(
        "SELECT toString(toDate(max(event_time))) FROM query_metrics_v2 "
        f"WHERE new_sha = '{pr_sha}' AND arch = '{perf_arch}' "
        f"AND pr_number = {pr_number} FORMAT TSV",
        "find the run date",
    )
    if not out:
        return None
    day = out.strip()
    return day if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day) else None


def fetch_reference_sha(
    pr_number: int, pr_sha: str, perf_arch: str, required: bool = True
) -> Optional[str]:
    """Find the reference (left/baseline) git SHA used in the CI run.

    The CI uploads each perf-test row to ``query_metrics_v2`` on
    play.clickhouse.com, which has both ``new_sha`` (the PR commit) and
    ``old_sha`` (the reference binary's git hash). That's the most reliable
    source: ``report.html`` shows ``clickhouse --version`` for the reference,
    which for official builds doesn't include the SHA, and ``left-commit.txt``
    inside ``logs.tar.zst`` has the same problem.

    A commit can be measured more than once (a rerun of the perf job after
    master moved on), and each run picks its own reference build, so the same
    ``(new_sha, arch)`` can carry several ``old_sha`` values. The S3 report we
    scrape is overwritten in place by the latest run, so the newest reference
    is the one that matches it -- order by ``event_time`` instead of taking an
    arbitrary row, and tell the user when the choice was not unique.

    ``required=False`` turns every failure into a warning and ``None``. Only
    the planning path (``--dry-run``) passes it: that path downloads nothing
    and runs nothing, so an unresolved reference SHA costs the user a line of
    the plan rather than a wrong measurement -- and it keeps the documented
    promise that a dry run needs nothing but ``python3`` and ``git``.
    """
    def fail(msg: str) -> None:
        if required:
            die(msg)
        log(f"WARNING: {msg}")

    query = (
        "SELECT old_sha, toString(max(event_time)) FROM query_metrics_v2 "
        f"WHERE new_sha = '{pr_sha}' AND arch = '{perf_arch}' "
        # Scoped to this pull request as well: the same commit measured under
        # another run (a master-track run uses pr_number = 0) would otherwise
        # be eligible, and "newest row wins" would then hand back a baseline
        # from a different comparison than the report being checked.
        f"AND pr_number = {pr_number} "
        "GROUP BY old_sha ORDER BY max(event_time) DESC FORMAT TSV"
    )
    try:
        out = subprocess.check_output(
            [
                "clickhouse",
                "client",
                "--host", "play.clickhouse.com",
                "--user", "explorer",
                "--secure",
                "--query", query,
            ],
            text=True,
            timeout=30,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        fail(
            "clickhouse client not found on PATH — required to query "
            "play.clickhouse.com for the reference SHA. Install it or pass "
            "--reference-sha explicitly."
        )
        return None
    except subprocess.CalledProcessError as e:
        fail(
            f"play.clickhouse.com query failed: {e.stderr.strip()}; "
            "pass --reference-sha explicitly"
        )
        return None
    rows = [line.split("\t") for line in out.splitlines() if line.strip()]
    if not rows or not re.fullmatch(r"[0-9a-f]{40}", rows[0][0]):
        fail(
            f"play.clickhouse.com returned no row for new_sha={pr_sha} "
            f"arch={perf_arch} pr={pr_number} (got: {out.strip()!r}); pass "
            "--reference-sha explicitly"
        )
        return None
    ref_sha = rows[0][0]
    if len(rows) > 1:
        log(
            f"WARNING: {len(rows)} perf runs found for {pr_sha} ({perf_arch}), "
            "each against a different reference build: "
            + ", ".join(f"{r[0][:12]} @ {r[1]}" for r in rows)
            + f". Using the newest ({ref_sha[:12]}), which is the one the "
            "scraped report reflects. Pass --reference-sha to override."
        )
    return ref_sha


# ---------------------------------------------------------------------------
# Binary download
# ---------------------------------------------------------------------------


def download_binaries(
    repo_root: Path,
    work_dir: Path,
    pr_number: int,
    pr_sha: str,
    ref_sha: str,
    build_type: str,
) -> tuple[Path, Path]:
    right_bin = work_dir / "right" / "clickhouse"
    left_bin = work_dir / "left" / "clickhouse"

    # A cached binary is only reusable when it is the one we were asked for.
    # The work dir defaults to a fixed path, so a second run for a different
    # commit (or with a different --reference-sha) would otherwise silently
    # benchmark the previous pair while the report prints the new SHAs. Each
    # download records what it is; a mismatch re-downloads.
    def cached_identity(binary: Path) -> Optional[str]:
        marker = binary.with_suffix(".identity")
        if not binary.is_file() or not marker.is_file():
            return None
        return marker.read_text().strip()

    def record_identity(binary: Path, identity: str) -> None:
        binary.with_suffix(".identity").write_text(identity + "\n")

    right_identity = f"{pr_sha} {build_type}"
    left_identity = f"{ref_sha} {build_type}"

    # Right binary (patched / PR)
    right_url = f"{BUILDS_BUCKET_PR}/PRs/{pr_number}/{pr_sha}/{PR_WORKFLOW_SEGMENT}/{build_type}/clickhouse"
    # Fallback for master-tip commits (no PR): same path under REFs/<branch>/<sha>
    candidate_right_urls = [right_url]
    candidate_right_urls.append(
        f"{BUILDS_BUCKET_MASTER}/REFs/master/{pr_sha}/{MASTER_WORKFLOW_SEGMENT}/{build_type}/clickhouse"
    )
    if cached_identity(right_bin) == right_identity:
        log(f"reusing cached patched binary for {pr_sha[:12]}")
    else:
        for u in candidate_right_urls:
            if http_head_ok(u):
                download(u, right_bin)
                break
        else:
            die(
                "patched binary not found at any of: "
                + ", ".join(candidate_right_urls)
            )
        right_bin.chmod(0o755)
        record_identity(right_bin, right_identity)

    # Left binary (reference / baseline) — always built off master
    left_url = f"{BUILDS_BUCKET_MASTER}/REFs/master/{ref_sha}/{MASTER_WORKFLOW_SEGMENT}/{build_type}/clickhouse"
    if cached_identity(left_bin) == left_identity:
        log(f"reusing cached reference binary for {ref_sha[:12]}")
    else:
        if not http_head_ok(left_url):
            die(f"reference binary not found at {left_url}")
        download(left_url, left_bin)
        left_bin.chmod(0o755)
        record_identity(left_bin, left_identity)

    return left_bin, right_bin


# ---------------------------------------------------------------------------
# Server setup (mirrors performance_tests.py)
# ---------------------------------------------------------------------------


# CPU pinning, ported from ci/jobs/performance_tests.py. On x86_64 CI pins both
# servers to one hyperthread per physical core and caps max_threads to that
# set, so query threads never share a hyperthread sibling depending on
# scheduler mood -- its top suspect for the amd-vs-arm A/A noise gap (0.51% vs
# 0.42%). A local rerun that skips this measures under noisier conditions than
# the report it is adjudicating, which is exactly how a real change ends up
# looking NOT REPRODUCED. arm (real cores only) is unchanged.
MAX_THREADS_OVERRIDE_FILE = "zzz-cpu-pinning-max-threads.xml"
MAX_THREADS_OVERRIDE_XML = """\
<!--
    Written by the double-check-perf-tests skill, x86_64 only, mirroring
    MAX_THREADS_OVERRIDE_XML in ci/jobs/performance_tests.py (arm keeps
    max_threads from perf-comparison-tweaks-users.xml).
-->
<clickhouse>
    <profiles>
        <default>
            <max_threads>{max_threads}</max_threads>
        </default>
    </profiles>
</clickhouse>
"""


def cpu_pinning_enabled(perf_arch: str) -> bool:
    """Pinning needs Linux (taskset, sysfs topology, sched_getaffinity), not
    just the CPU family: an x86_64 macOS run must not get a taskset prefix it
    cannot execute."""
    return perf_arch == "amd" and os.uname().sysname == "Linux"


def get_physical_core_cpu_list() -> str:
    """Return a `taskset -c` CPU list with one hyperthread per physical core.

    Parses /sys/devices/system/cpu/cpu*/topology/thread_siblings_list, keeps
    the first *allowed* sibling of each unique pair (intersected with the
    process affinity mask, since sysfs exposes the host topology and taskset
    fails on a disallowed CPU), and falls back to every allowed CPU when the
    topology cannot be read."""
    getaffinity = getattr(os, "sched_getaffinity", None)
    try:
        allowed = getaffinity(0) if getaffinity else None
    except OSError:
        allowed = None
    cores: dict[int, int] = {}
    for path in Path("/sys/devices/system/cpu").glob(
        "cpu[0-9]*/topology/thread_siblings_list"
    ):
        # Formats seen in the wild: "0,8", "0-1", "0" (no SMT). One unreadable
        # file must not discard the rest of the topology.
        try:
            siblings = [
                int(x) for x in re.split(r"[,-]", path.read_text().strip()) if x
            ]
        except (OSError, ValueError):
            continue
        usable = [c for c in siblings if allowed is None or c in allowed]
        if usable:
            cores[min(siblings)] = min(usable)
    cpus = set(cores.values())
    if not cpus:
        # Halving without topology would be a guess that drops real cores on
        # non-SMT hosts. Keep every allowed CPU: hyperthread sharing is then
        # possible, but no measurement is skewed by idling half the cores.
        log(
            "WARNING: could not parse cpu topology from sysfs; using all "
            "allowed cpus (sibling pairs unknown, hyperthread sharing possible)"
        )
        cpus = set(allowed) if allowed else set(range(os.cpu_count() or 2))
    return ",".join(str(cpu) for cpu in sorted(cpus))


def write_max_threads_override(side_dir: Path, max_threads: int) -> None:
    """Cap max_threads at the number of pinned CPUs, one query thread per
    pinned CPU. The zzz- prefix makes it sort after the static users.d files
    copied from tests/performance/scripts/config."""
    target = side_dir / "config" / "users.d" / MAX_THREADS_OVERRIDE_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(MAX_THREADS_OVERRIDE_XML.format(max_threads=max_threads))


def link_clickhouse_tools(side_dir: Path) -> None:
    """Create the clickhouse-{server,client,local,keeper} symlinks next to the
    main binary, the way CHServer expects."""
    binary = side_dir / "clickhouse"
    for name in ("clickhouse-server", "clickhouse-client", "clickhouse-local", "clickhouse-keeper"):
        target = side_dir / name
        if target.is_symlink() or target.exists():
            target.unlink()
        target.symlink_to(binary.name)


# The reference (left) binary is an older master build. A config drop-in in
# this checkout may set a server setting that build predates, which it rejects
# with UNKNOWN_SETTING and then fails to start -- before a single query runs.
# ci/jobs/performance_tests.py strips such settings from keeper_port.xml with a
# sed for exactly this reason (both sides must share an identical config
# anyway, and these values are irrelevant to query performance). Keep this list
# in sync with the seds in the INSTALL_CLICKHOUSE stage of that job.
CONFIG_SETTINGS_TO_STRIP = {
    "keeper_port.xml": ["log_readahead_commit_window_bytes"],
}


def strip_incompatible_settings(config_d: Path) -> None:
    """Drop settings the reference binary may not know from the copied config
    drop-ins. Mirrors `sed -i '/<setting>/d' <file>` in the CI job."""
    for file_name, settings in CONFIG_SETTINGS_TO_STRIP.items():
        target = config_d / file_name
        if not target.is_file():
            continue
        lines = target.read_text().splitlines(keepends=True)
        kept = [
            line for line in lines
            if not any(f"<{setting}>" in line for setting in settings)
        ]
        if len(kept) != len(lines):
            dropped = len(lines) - len(kept)
            log(
                f"stripped {dropped} line(s) from {file_name} that the "
                "reference binary may reject: " + ", ".join(settings)
            )
            target.write_text("".join(kept))


def prepare_configs(repo_root: Path, side_dir: Path) -> None:
    cfg = side_dir / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    # Base server config.xml + users.xml + drop-ins from programs/server.
    shutil.copy(repo_root / "programs/server/config.xml", cfg / "config.xml")
    shutil.copy(repo_root / "programs/server/users.xml", cfg / "users.xml")
    src_cfgd = repo_root / "programs/server/config.d"
    dst_cfgd = cfg / "config.d"
    if dst_cfgd.exists():
        shutil.rmtree(dst_cfgd)
    shutil.copytree(src_cfgd, dst_cfgd, symlinks=False)

    # Overlay performance-test specific config drop-ins.
    perf_cfgd = repo_root / "tests/performance/scripts/config/config.d"
    if perf_cfgd.is_dir():
        for f in perf_cfgd.glob("*.xml"):
            shutil.copy(f, dst_cfgd / f.name)

    # users.d
    perf_users_d = repo_root / "tests/performance/scripts/config/users.d"
    dst_users_d = cfg / "users.d"
    if dst_users_d.exists():
        shutil.rmtree(dst_users_d)
    if perf_users_d.is_dir():
        shutil.copytree(perf_users_d, dst_users_d, symlinks=False)

    # Files that conflict between two co-hosted servers; CI strips these.
    for to_remove in (
        "storage_conf_local.xml",  # collides on /var/lib/clickhouse fscache dirs
        "text_log.xml",
        "memory_profiler.yaml",
        "serverwide_trace_collector.xml",
        "jemalloc_flush_profile.yaml",
        "keeper_max_request_size.xml",
        "backups.xml",
        "ssh.xml",
    ):
        f = dst_cfgd / to_remove
        if f.exists():
            f.unlink()

    strip_incompatible_settings(dst_cfgd)


def hardlink_db(db_source: Path, side_db: Path) -> None:
    if side_db.exists():
        shutil.rmtree(side_db)
    # cp -al = hardlink-recursive — fast and saves space
    subprocess.run(["cp", "-al", str(db_source), str(side_db)], check=True)
    # Remove preprocessed configs + system tables so each server owns its own
    for p in ("preprocessed_configs", "data/system", "metadata/system", "status"):
        victim = side_db / p
        if victim.is_dir():
            shutil.rmtree(victim, ignore_errors=True)
        elif victim.exists():
            victim.unlink()


def seed_user_files(repo_root: Path, side_db: Path) -> None:
    """Symlink tests/performance/user_files/* into the server's user_files
    directory, the way the Configure stage of ci/jobs/performance_tests.py
    does. Tests such as json_file_query read `file('instances.json')`, which
    resolves under user_files_path; without this they fail locally while CI
    runs them fine."""
    src = repo_root / "tests/performance/user_files"
    dst = side_db / "user_files"
    dst.mkdir(parents=True, exist_ok=True)
    if not src.is_dir():
        return
    for f in sorted(src.iterdir()):
        link = dst / f.name
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(f.resolve())


def cleanup_user_files(side_dbs: list[Path]) -> None:
    """Remove everything a test wrote into user_files, keeping the seeded
    fixture symlinks.

    Tests write there with `INSERT INTO FUNCTION file(...)` (parquet_read,
    json_type_parsing, insert_values_with_expressions, ...) and `drop_query`
    only drops tables, so without this a later XML can read a file an earlier
    one left behind and the results become order-dependent. CI runs the same
    cleanup after every test."""
    for side_db in side_dbs:
        user_files = side_db / "user_files"
        if not user_files.is_dir():
            continue
        for entry in user_files.iterdir():
            if entry.is_symlink():
                continue
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink()


def prepare_dataset(db_source: Path, binary_for_preconfig: Path,
                    config_dir: Path, top_level_domains: Path,
                    work_dir: Path, create_test_hits: bool = True) -> None:
    """Ensure the shared dataset directory has the bookkeeping the perf
    framework expects.

    - ``default`` and ``datasets`` are Ordinary databases (always needed).
    - If ``create_test_hits`` and ``datasets.hits_v1`` exists and ``test.hits``
      doesn't, do ``CREATE DATABASE test; RENAME TABLE datasets.hits_v1 TO
      test.hits`` via a temporary clickhouse-server pointed at ``db_source``.
      Under ``--populate`` this is skipped: each side builds its own
      ``test.hits`` from ``datasets.hits_v1`` instead, so the source table has
      to stay where it is.

    *Why a temp server and not a filesystem rename?* Filesystem moves of
    metadata files leave ClickHouse's per-table CREATE-query state in an
    inconsistent enough form that loading other databases (specifically
    ``tpcds`` in our case) hits a NULL pointer in
    ``DatabaseOrdinary::getConvertToReplicatedFlagPath`` when running
    against the hardlinked copy. SQL ``RENAME TABLE`` performs the
    transition cleanly.

    Note this differs from ``ci/jobs/performance_tests.py``, which builds
    ``test.hits`` with ``INSERT SELECT`` on each server separately -- that is
    what ``--populate`` reproduces. The rename here is the cheap default: one
    shared copy of the data, hardlinked into both sides.

    Finally we strip ``data/system``, ``metadata/system``,
    ``preprocessed_configs`` and ``status`` from db_source — those are
    server-instance state that shouldn't be hardlinked between two
    co-hosted servers.
    """
    meta = db_source / "metadata"
    meta.mkdir(parents=True, exist_ok=True)
    (meta / "default.sql").write_text("ATTACH DATABASE default ENGINE=Ordinary\n")
    (meta / "datasets.sql").write_text("ATTACH DATABASE datasets ENGINE=Ordinary\n")

    needs_rename = (
        create_test_hits
        and (db_source / "data" / "datasets" / "hits_v1").is_dir()
        and not (db_source / "data" / "test" / "hits").exists()
    )
    if needs_rename:
        log(
            "running preconfig server on db0 to create test.hits "
            "(SQL: CREATE DATABASE test; RENAME TABLE datasets.hits_v1 TO test.hits)"
        )
        ensure_ports_free({
            PRECONFIG_TCP: "preconfig TCP",
            PRECONFIG_KEEPER_TCP: "preconfig keeper",
            PRECONFIG_KEEPER_RAFT: "preconfig keeper raft",
            PRECONFIG_INTERSERVER: "preconfig interserver",
        })
        coord = work_dir / "coordination0"
        # Keeper state is only ever valid for the data it was written against.
        # The work dir is reused between runs, so start from an empty one.
        shutil.rmtree(coord, ignore_errors=True)
        coord.mkdir(parents=True, exist_ok=True)
        preconfig_log = work_dir / "preconfig.log"
        with open(preconfig_log, "w") as lf:
            cmd = [
                str(binary_for_preconfig.parent / "clickhouse-server"),
                "--config-file=" + str(config_dir / "config.xml"),
                "--",
                "--path", str(db_source),
                "--user_files_path", str(db_source / "user_files"),
                "--top_level_domains_path", str(top_level_domains),
                "--keeper_server.storage_path", str(coord),
                "--tcp_port", str(PRECONFIG_TCP),
                "--keeper_server.tcp_port", str(PRECONFIG_KEEPER_TCP),
                "--keeper_server.raft_configuration.server.port",
                str(PRECONFIG_KEEPER_RAFT),
                "--zookeeper.node.port", str(PRECONFIG_KEEPER_TCP),
                "--interserver_http_port", str(PRECONFIG_INTERSERVER),
            ]
            proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
        client = binary_for_preconfig.parent / "clickhouse-client"
        try:
            # Wait for the preconfig server to come up
            ready = False
            for _ in range(30):
                try:
                    out = subprocess.check_output(
                        [str(client), "--port", str(PRECONFIG_TCP),
                         "--query", "select 1"],
                        text=True, timeout=5, stderr=subprocess.DEVNULL,
                    )
                    if out.strip() == "1":
                        ready = True
                        break
                except Exception:
                    pass
                if proc.poll() is not None:
                    break
                time.sleep(2)
            if not ready:
                die(
                    f"preconfig server failed to come up; see {preconfig_log}"
                )
            # Belt and braces on top of the dedicated port: confirm the
            # server answering is the one we started, over the dataset we mean
            # to modify. RENAME TABLE against someone else's server would move
            # their data.
            server_path = subprocess.check_output(
                [str(client), "--port", str(PRECONFIG_TCP), "--query",
                 "SELECT value FROM system.server_settings WHERE name = 'path'"],
                text=True, timeout=30,
            ).strip().rstrip("/")
            if server_path != str(db_source).rstrip("/"):
                die(
                    f"the server on port {PRECONFIG_TCP} serves {server_path!r}, "
                    f"not the dataset {str(db_source)!r} we started it for. "
                    "Refusing to rename tables on it."
                )
            for sql in (
                "CREATE DATABASE IF NOT EXISTS test",
                "RENAME TABLE datasets.hits_v1 TO test.hits",
            ):
                subprocess.run(
                    [str(client), "--port", str(PRECONFIG_TCP), "--query", sql],
                    check=True, timeout=30,
                )
            log("preconfig: test.hits created")
        finally:
            try:
                proc.terminate()
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    # Strip server-instance state that must not be shared via hardlinks.
    for victim in ("preprocessed_configs", "status",
                   "data/system", "metadata/system"):
        p = db_source / victim
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink()


@dataclass
class ServerHandle:
    name: str
    side_dir: Path
    port: int
    http_port: int
    keeper_port: int
    raft_port: int
    interserver_port: int
    proc: Optional[subprocess.Popen] = field(default=None)
    log_path: Path = field(default=Path("/dev/null"))


def ensure_ports_free(ports: dict[int, str]) -> None:
    """Fail early and clearly if any port we are about to bind is taken.

    The HTTP ports mirror CI (8123 / 18123) and 8123 is the default HTTP port
    of an ordinary local clickhouse-server, so a collision on a development
    machine is likely enough to deserve its own message rather than a bind
    error buried in the server log."""
    busy = []
    for port, what in sorted(ports.items()):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                busy.append(f"{port} ({what})")
    if busy:
        die(
            "these ports are already in use: " + ", ".join(busy)
            + ". Stop whatever is listening (a local clickhouse-server, or a "
            "previous run of this script) and try again."
        )


def start_server(
    side_dir: Path,
    name: str,
    port: int,
    http_port: int,
    keeper_port: int,
    raft_port: int,
    interserver_port: int,
    top_level_domains: Path,
    cpu_list: Optional[str] = None,
) -> ServerHandle:
    log_path = side_dir / "server.log"
    log_fh = open(log_path, "w")
    # Both servers get the *same* pinned CPU list: they are measured
    # alternately, not concurrently.
    taskset_prefix = ["taskset", "-c", cpu_list] if cpu_list else []
    cmd = [
        *taskset_prefix,
        str(side_dir / "clickhouse-server"),
        "--config-file=" + str(side_dir / "config" / "config.xml"),
        "--",
        "--path", str(side_dir / "db"),
        "--user_files_path", str(side_dir / "db" / "user_files"),
        "--top_level_domains_path", str(top_level_domains),
        "--tcp_port", str(port),
        # The perf-comparison drop-in removes <http_port>, so it has to come
        # back on the command line; shell-script queries reach the server over
        # $CLICKHOUSE_URL. Same as CHServer in ci/jobs/performance_tests.py.
        "--http_port", str(http_port),
        "--keeper_server.tcp_port", str(keeper_port),
        "--keeper_server.raft_configuration.server.port", str(raft_port),
        "--keeper_server.storage_path", str(side_dir / "coordination"),
        "--zookeeper.node.port", str(keeper_port),
        "--interserver_http_port", str(interserver_port),
        # Denser than the default: perf profiles single queries in isolation.
        # Same value as CHServer in ci/jobs/performance_tests.py.
        "--jemalloc_profiler_sampling_rate", "16",
    ]
    log(f"starting {name} server on TCP {port}: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT)
    handle = ServerHandle(
        name=name,
        side_dir=side_dir,
        port=port,
        http_port=http_port,
        keeper_port=keeper_port,
        raft_port=raft_port,
        interserver_port=interserver_port,
        proc=proc,
        log_path=log_path,
    )
    return handle


def server_query(h: ServerHandle, sql: str, timeout: int = 30) -> str:
    """Run a SQL query against this server's clickhouse-client. Returns the
    stripped stdout. Raises on any error."""
    client = h.side_dir / "clickhouse-client"
    out = subprocess.check_output(
        [str(client), "--port", str(h.port), "--query", sql],
        text=True,
        timeout=timeout,
    )
    return out.strip()


# ---------------------------------------------------------------------------
# Dataset population (--populate)
# ---------------------------------------------------------------------------

# Same knobs as _perf_client in ci/jobs/performance_tests.py: hits_100m_single
# alone needs ~21 GiB for the insert, and OPTIMIZE FINAL on it is slow enough
# to trip the default execution-time limits.
POPULATE_CLIENT_SETTINGS = [
    "--max_memory_usage", "30G",
    "--max_memory_usage_for_user", "30G",
    "--max_estimated_execution_time", "0",
    "--max_execution_time", "1800",
    "--receive_timeout", "1800",
]
POPULATE_INSERT_SETTINGS = (
    "enable_filesystem_cache_on_write_operations=0, max_insert_threads=16"
)

def populate_query(h: ServerHandle, sql: str, timeout: int = 2000) -> str:
    client = h.side_dir / "clickhouse-client"
    out = subprocess.check_output(
        [str(client), "--port", str(h.port), *POPULATE_CLIENT_SETTINGS,
         "--query", sql],
        text=True,
        timeout=timeout,
    )
    return out.strip()


def table_exists(h: ServerHandle, table: str) -> bool:
    return populate_query(h, f"EXISTS TABLE {table}", timeout=60) == "1"


def rebuild_table(h: ServerHandle, source: str, destination: str) -> None:
    """Re-insert an attached dataset through this server so its parts are
    written by *this* binary and its settings (sparse columns, statistics, mark
    format) instead of the frozen tarball format, then OPTIMIZE FINAL back to a
    single part matching the original layout.

    Mirrors ``rebuild_table`` in ``ci/jobs/performance_tests.py``. INSERT is
    what recomputes serialization; a bare OPTIMIZE would inherit the source
    parts' serialization, so it cannot replace the insert. An in-place rebuild
    goes through a temporary name and is swapped in with RENAME (the datasets
    live in Ordinary databases, so EXCHANGE TABLES is not available)."""
    if not table_exists(h, source):
        die(f"{h.name}: cannot rebuild {destination}: source {source} is not attached")
    target = f"{destination}_rebuild" if source == destination else destination
    log(f"{h.name}: rebuilding {destination} from {source}")
    populate_query(h, f"DROP TABLE IF EXISTS {target} SYNC")
    populate_query(h, f"CREATE TABLE {target} AS {source}")
    populate_query(
        h,
        f"INSERT INTO {target} SELECT * FROM {source} "
        f"SETTINGS {POPULATE_INSERT_SETTINGS}",
    )
    populate_query(h, f"OPTIMIZE TABLE {target} FINAL")
    if target != destination:
        old = f"{destination}_old"
        populate_query(h, f"DROP TABLE IF EXISTS {old} SYNC")
        populate_query(
            h, f"RENAME TABLE {destination} TO {old}, {target} TO {destination}"
        )
        populate_query(h, f"DROP TABLE {old} SYNC")
    else:
        populate_query(h, f"DROP TABLE {source} SYNC")
    log(f"{h.name}: {destination} rebuilt")


def populate_side(h: ServerHandle, tables: list[str]) -> None:
    """Rebuild the requested hits tables on one server, sequentially.

    Sequential on purpose: the inserts share the per-user memory limit and
    hits_100m_single alone uses ~21 GiB, so running them in parallel on one
    server gets killed by the OvercommitTracker."""
    # No completion marker: main re-hardlinks each side's db from the dataset
    # directory at the start of every run, which would wipe one anyway, so
    # --populate rebuilds every time it is asked for.
    populate_query(h, "CREATE DATABASE IF NOT EXISTS test")
    for table in tables:
        if table == "test.hits":
            # Freshly extracted tarball: the source is still datasets.hits_v1.
            # A work dir carried over from a default (hardlink) run already has
            # it renamed to test.hits, in which case rebuild it in place.
            if table_exists(h, "datasets.hits_v1"):
                rebuild_table(h, "datasets.hits_v1", "test.hits")
            else:
                rebuild_table(h, "test.hits", "test.hits")
        else:
            rebuild_table(h, table, table)



def populate_data_both(handles: list[ServerHandle], tables: list[str]) -> None:
    """Populate both servers in parallel. Each writes its own parts, so a PR
    that changes a write-time default is reflected only on the right (patched)
    side -- which is the whole point of doing this instead of sharing one
    hardlinked copy."""
    log(f"populating datasets on both sides: {', '.join(tables)}")
    errors: list[BaseException] = []

    def run(h: ServerHandle) -> None:
        try:
            populate_side(h, tables)
        except BaseException as e:  # noqa: BLE001
            log(f"{h.name}: populate failed: {e}")
            errors.append(e)

    threads = [Thread(target=run, args=(h,)) for h in handles]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        die(f"dataset population failed ({len(errors)} of {len(handles)} sides)")
    log("population done on both sides")


def wait_for_merges(
    handles: list[ServerHandle],
    quiet_window_s: int = 120,
    max_seconds: int = 1800,
) -> None:
    """Wait until no NEW merge has been scheduled for ~``quiet_window_s``.

    Freshly-hardlinked dataset directories carry over a snapshot of parts
    at whatever merge level they were taken; ClickHouse queues consolidation
    merges at startup. Measuring before those settle makes results unstable
    — the part count drifts (changes plans, prefetch, cache reuse on
    external storage), and the merge threads compete for CPU and I/O with
    the queries we time.

    "No new merge scheduled recently" is a better signal than "no merges
    in flight": some merges are long-running, and waiting for an empty
    ``system.merges`` could stretch the wait by tens of minutes for no
    actual measurement benefit once the *rate* of new merges has dropped.
    We use ``min(elapsed)`` over ``system.merges`` as the proxy — if the
    youngest in-flight merge has been running for at least ``quiet_window_s``
    seconds, then nothing newer has been scheduled in that window. When
    the table is empty we treat it as fully quiet.
    """
    deadline = time.monotonic() + max_seconds
    log(
        f"wait_for_merges: waiting until no new merge has started "
        f"in the last {quiet_window_s}s on either server "
        f"(timeout {max_seconds}s)"
    )
    last_report = 0.0
    while time.monotonic() < deadline:
        worst_youngest = None  # smallest elapsed across both servers
        max_in_flight = 0
        for h in handles:
            try:
                row = server_query(
                    h,
                    # Use a large sentinel when there are no in-flight merges
                    # so 'youngest' is large enough to satisfy the quiet
                    # window. min() on an empty set in ClickHouse returns
                    # the default value (0), not NULL, so ifNull alone
                    # doesn't help.
                    "SELECT count(), if(count() = 0, 1e9, min(elapsed)) "
                    "FROM system.merges FORMAT TSV",
                )
                n_str, youngest_str = row.split("\t")
                n = int(n_str)
                youngest = float(youngest_str)
            except Exception as e:
                log(f"wait_for_merges: {h.name} query failed: {e}")
                n, youngest = 1, 0.0
            max_in_flight = max(max_in_flight, n)
            if worst_youngest is None or youngest < worst_youngest:
                worst_youngest = youngest
        if worst_youngest is not None and worst_youngest >= quiet_window_s:
            log(
                f"wait_for_merges: youngest merge across both servers is "
                f"{worst_youngest:.0f}s old, in_flight={max_in_flight} — "
                f"considered settled, proceeding"
            )
            return
        # Periodic progress line so the user knows we haven't hung
        now = time.monotonic()
        if now - last_report > 30:
            log(
                f"wait_for_merges: in_flight (max across servers)="
                f"{max_in_flight}, youngest_elapsed="
                f"{worst_youngest:.0f}s (need >={quiet_window_s}s)"
            )
            last_report = now
        time.sleep(5)
    log(
        f"wait_for_merges: timeout after {max_seconds}s, proceeding anyway "
        "(measurements may be noisy)"
    )


def server_start_failure(h: ServerHandle) -> str:
    """Explain why a server did not come up, so an unknown setting is named
    instead of leaving the user to dig through the log.

    Worth singling out: CONFIG_SETTINGS_TO_STRIP is a hardcoded list mirroring
    the CI job, so it goes stale the moment master adds another setting the
    reference build predates."""
    msg = f"{h.name} server failed to start; see {h.log_path}"
    try:
        text = h.log_path.read_text(errors="replace")
    except OSError:
        return msg
    for line in text.splitlines():
        if "UNKNOWN_SETTING" in line or "Unknown setting" in line:
            return (
                f"{msg}\n  {line.strip()}\n"
                "  The reference binary predates a setting in this checkout's "
                "config. Add it to CONFIG_SETTINGS_TO_STRIP (and check whether "
                "ci/jobs/performance_tests.py strips it too)."
            )
    return msg


def wait_server_ready(h: ServerHandle, attempts: int = 30, delay: float = 2.0) -> bool:
    client = h.side_dir / "clickhouse-client"
    for _ in range(attempts):
        try:
            out = subprocess.check_output(
                [str(client), "--port", str(h.port), "--query", "select 1"],
                text=True,
                timeout=5,
                stderr=subprocess.DEVNULL,
            )
            if out.strip() == "1":
                return True
        except Exception:
            pass
        if h.proc and h.proc.poll() is not None:
            return False
        time.sleep(delay)
    return False


def stop_server(h: ServerHandle) -> None:
    if h.proc is None:
        return
    try:
        h.proc.terminate()
        try:
            h.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            h.proc.kill()
            h.proc.wait()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Test running
# ---------------------------------------------------------------------------


def run_perf_test(
    repo_root: Path,
    work_dir: Path,
    test_xml: str,
    query_indices: list[int],
    runs: Optional[int],
) -> tuple[Path, int]:
    """Run perf.py for one XML, restricted to the given query indices.

    Returns the raw TSV path and perf.py's exit code. A failed run must not be
    quietly reported as "no local data": that reads as "CI's change did not
    reproduce" when in fact nothing was measured."""
    out_path = work_dir / "raw" / f"{Path(test_xml).stem}-raw.tsv"
    err_path = work_dir / "raw" / f"{Path(test_xml).stem}-err.log"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # NB: --queries-to-run has nargs='*' in perf.py, so it greedily consumes
    # everything that follows including the XML positional. Pass the file
    # path first to avoid that.
    cmd = [
        sys.executable,
        str(repo_root / "tests/performance/scripts/perf.py"),
        str(repo_root / "tests/performance" / test_xml),
        "--host", "localhost", "localhost",
        "--port", str(LEFT_TCP), str(RIGHT_TCP),
        # <query type="shell"> queries build $CLICKHOUSE_BINARY / $CLICKHOUSE_LOCAL
        # / $CLICKHOUSE_URL out of these. Without them perf.py falls back to
        # `clickhouse` on $PATH and port 8123, which would measure the same
        # unrelated binary on both sides and silently report "no change".
        "--binary", str(work_dir / "left" / "clickhouse"),
        str(work_dir / "right" / "clickhouse"),
        "--http-port", str(LEFT_HTTP), str(RIGHT_HTTP),
        # Omitted unless asked for, exactly as CHServer.run_test does.
        *(["--runs", str(runs)] if runs is not None else []),
        # CI passes 10 here; the profile runs happen after a query's diff is
        # computed, so they cannot change its numbers, and this skill does not
        # collect flamegraphs.
        "--profile-seconds", "0",
        "--queries-to-run", *[str(i) for i in query_indices],
    ]
    log(f"running perf.py on {test_xml} queries={query_indices}: {' '.join(cmd)}")
    with open(out_path, "w") as out_fh, open(err_path, "w") as err_fh:
        proc = subprocess.run(cmd, stdout=out_fh, stderr=err_fh, check=False)
    if proc.returncode != 0:
        log(f"ERROR: perf.py exited {proc.returncode} for {test_xml}; see {err_path}")
    return out_path, proc.returncode


def load_stat_threshold(repo_root: Path):
    """Return perf.py's own `stat_threshold` function.

    compare.sh confirms a flagged query with `abs(diff) > changed_threshold and
    abs(diff) >= stat_threshold`, where stat_threshold is the q99 of the
    balanced-split null (eqmed.sql) recomputed from the rerun's own per-run
    samples. perf.py carries a Python implementation of that same randomization
    test -- it drives its adaptive stop -- so the gate is reproducible here
    exactly, with no second implementation to drift.

    The function is lifted out of perf.py by AST rather than imported: perf.py
    runs a benchmark at module scope, so importing it is not an option. If the
    lift fails the run stops -- silently falling back to a different statistic
    would mean printing verdicts computed by a rule that is not CI's."""
    src_path = repo_root / "tests/performance/scripts/perf.py"
    try:
        tree = ast.parse(src_path.read_text())
    except (OSError, SyntaxError) as e:
        die(f"cannot read {src_path} to lift stat_threshold: {e}")
    wanted_defs = {"ch_median", "stat_threshold"}
    wanted_consts = {"MAX_EXACT_SPLIT_RUNS", "SAMPLED_SPLITS"}
    picked: list[ast.stmt] = []
    found: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted_defs:
            picked.append(node)
            found.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in wanted_consts:
                    picked.append(node)
                    found.add(target.id)
    missing = (wanted_defs | wanted_consts) - found
    if missing:
        die(
            f"could not lift {', '.join(sorted(missing))} from {src_path}; "
            "perf.py changed shape. Update load_stat_threshold -- the local "
            "rerun must be judged by the same gate as CI."
        )
    namespace: dict = {"itertools": itertools, "random": random}
    exec(compile(ast.Module(body=picked, type_ignores=[]), str(src_path), "exec"), namespace)
    return namespace["stat_threshold"]


def parse_perf_runs(raw_path: Path) -> dict[int, dict[int, list[float]]]:
    """Per-run timings from perf.py output: `query <qi> <run_id> <conn> <s>`.

    Connection 0 is the left (reference) server. These are the same lines
    compare.sh collects from the rerun's raw TSV to recompute the statistics
    (`sed -n "s/^query\t/.../p"`)."""
    runs: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    if not raw_path.is_file():
        return {}
    for line in raw_path.read_text().splitlines():
        if not line.startswith("query\t"):
            continue
        parts = line.split("\t")
        if len(parts) < 5:
            continue
        try:
            runs[int(parts[1])][int(parts[3])].append(float(parts[4]))
        except ValueError:
            continue
    return {qi: dict(sides) for qi, sides in runs.items()}


def parse_perf_diffs(raw_path: Path) -> dict[int, dict]:
    """Extract per-query diffs from perf.py output.

    Lines we care about:
      diff\tQI\tleft_median\tright_median\trel_diff\tpvalue
      median\tQI\tleft_median   (when only one side ran)
      client-time\tQI\tclient_sec\tserver_sec   (we ignore — not the comparison)
    """
    result: dict[int, dict] = {}
    if not raw_path.is_file():
        return result
    for line in raw_path.read_text().splitlines():
        if line.startswith("diff\t"):
            parts = line.split("\t")
            if len(parts) >= 6:
                try:
                    qi = int(parts[1])
                    result[qi] = {
                        "left_median": float(parts[2]),
                        "right_median": float(parts[3]),
                        "rel_diff": float(parts[4]),
                        "pvalue": float(parts[5]),
                    }
                except ValueError:
                    pass
    return result


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


# compare.sh flags a query when `abs(diff) > changed_threshold and abs(diff) >=
# stat_threshold`, where changed_threshold is per query: the 0.15 floor raised
# by the historical p99 and the per-test threshold. The floor is deliberately
# above run-to-run noise - micro benchmarks swing 10-15% between two binaries
# from machine noise and code-layout artifacts alone - so a local rerun must
# clear the same bar before it can claim to have reproduced anything.
CHANGED_THRESHOLD_FLOOR = 0.15
# The noise gate is CI's own: `abs(diff) >= stat_threshold`, non-strict, with
# stat_threshold recomputed from the rerun's per-run samples (see
# load_stat_threshold). The p-value perf.py reports is displayed but not used
# to decide -- it is a different test from the one the CI gate applies.


def fmt_diff(diff: float) -> str:
    sign = "+" if diff > 0 else ""
    return f"{sign}{diff * 100:.1f}%"


def fmt_sec(v: float) -> str:
    if v >= 1:
        return f"{v:.3f}s"
    if v >= 1e-3:
        return f"{v * 1e3:.1f}ms"
    return f"{v * 1e6:.0f}us"


def print_report(
    changed: list[ChangedQuery],
    local_results: dict[tuple[str, int], dict],
    local_arch: str,
    failed_tests: Optional[dict[str, int]] = None,
    unreadable: Optional[list[tuple[str, int, int, str]]] = None,
    local_arch_measured: bool = True,
    errored_tests: Optional[set[str]] = None,
) -> None:
    print()
    print("=" * 120)
    print("Local double-check vs. CI report")
    print("=" * 120)
    header = (
        f"{'TEST':<32} {'Q#':>3}  CI@   "
        f"{'CI old':>8} {'CI new':>8} {'CI Δ':>8} | "
        f"{'L old':>8} {'L new':>8} {'L Δ':>8} {'pval':>6}  RESULT"
    )
    print(header)
    print("-" * len(header))
    confirmed = 0
    not_reproduced = 0
    failed = 0
    errored = 0
    unverifiable = 0

    def print_arch_disagreement(cq: ChangedQuery) -> bool:
        """CI's own verdict split across arches. The table shows one arch's
        numbers per query, so a query CI called slower on one arch and faster
        on the other would otherwise read as a single-direction change."""
        rows = cq.ci_by_arch
        if len({r["direction"] for r in rows.values()}) < 2:
            return False
        print(
            f"{'':<32} {'':>3}  └ CI split: "
            + ", ".join(
                f"{a} {fmt_diff(r['diff'])} {r['direction']}"
                for a, r in rows.items()
            )
        )
        return True

    split_arch = False
    for cq in sorted(changed, key=lambda c: (c.test, c.query_index, c.arch)):
        key = (cq.test, cq.query_index)
        local = local_results.get(key)
        ci_dir = cq.direction

        # "CI@" tag — short marker telling the reader which arch(es) CI
        # flagged this query on. "X-only" means CI saw the change on that
        # arch and not on ours; bare arch means we share the verdict.
        archs = cq.flagged_on or [cq.arch]
        if local_arch in archs and len(archs) == 1:
            ci_at = local_arch
        elif local_arch in archs:
            ci_at = "+".join(archs)
        else:
            # local arch did NOT flag this; CI saw it elsewhere
            ci_at = "/".join(archs) + "-only"
        if cq.ci_unconfirmed:
            ci_at += "*"

        if local is None and (failed_tests or {}).get(cq.test) is not None:
            verdict = (
                f"perf.py FAILED (exit {failed_tests[cq.test]}), NOT MEASURED "
                f"— see raw/{cq.test}-err.log"
            )
            local_str = f"{'':>8} {'':>8} {'':>8} {'':>6}"
            failed += 1
        elif local is None and cq.test in (errored_tests or set()):
            # perf.py skips a query that failed on *every* server and exits 0
            # (`if len(no_errors) == 0: continue`), printing only a traceback
            # on stderr -- so this is indistinguishable from a missing row
            # unless the per-test stderr is read. It is not an absence of
            # data, it is a query that could not run.
            verdict = (
                f"query ERRORED locally, NOT MEASURED — perf.py skipped it "
                f"after it failed on every server; see raw/{cq.test}-err.log"
            )
            local_str = f"{'':>8} {'':>8} {'':>8} {'':>6}"
            errored += 1
        elif local is None:
            verdict = "no local data"
            local_str = f"{'':>8} {'':>8} {'':>8} {'':>6}"
        else:
            l_old = local["left_median"]
            l_new = local["right_median"]
            l_rel = local["rel_diff"]
            l_pv = local["pvalue"]
            local_str = (
                f"{fmt_sec(l_old):>8} {fmt_sec(l_new):>8} "
                f"{fmt_diff(l_rel):>8} {l_pv:>6.3f}"
            )
            same_direction = (l_rel > 0 and ci_dir == "slower") or (
                l_rel < 0 and ci_dir == "faster"
            )
            if cq.threshold_unknown:
                verdict = (
                    "NO VERDICT — CI's per-query threshold for this demoted "
                    "query could not be recovered"
                )
                unverifiable += 1
                print(
                    f"{cq.test[:32]:<32} {cq.query_index:>3}  {ci_at:<6}"
                    f"{fmt_sec(cq.left):>8} {fmt_sec(cq.right):>8} "
                    f"{fmt_diff(cq.diff):>8} | {local_str}  {verdict}"
                )
                split_arch |= print_arch_disagreement(cq)
                continue
            bar = cq.changed_threshold or CHANGED_THRESHOLD_FLOOR
            l_stat = local.get("stat_threshold")
            confirmed_here = (
                same_direction
                and abs(l_rel) > bar
                and l_stat is not None
                and abs(l_rel) >= l_stat
            )
            if confirmed_here:
                if local_arch in archs:
                    verdict = f"CONFIRMED {ci_dir}"
                else:
                    # CI flagged it on another arch; local *also* shows
                    # a same-direction delta — interesting, worth noting.
                    verdict = (
                        f"CONFIRMED {ci_dir} "
                        f"(local mirrors {'/'.join(archs)} regression)"
                    )
                confirmed += 1
            else:
                if not same_direction:
                    why = "opposite direction"
                elif abs(l_rel) <= bar:
                    why = f"|Δ| {abs(l_rel):.1%} <= threshold {bar:.0%}"
                elif l_stat is None:
                    why = "no stat_threshold (too few runs)"
                else:
                    why = f"|Δ| {abs(l_rel):.1%} < local noise {l_stat:.1%}"
                if local_arch in archs:
                    verdict = f"NOT REPRODUCED ({ci_dir} in CI; {why})"
                else:
                    verdict = (
                        f"NOT REPRODUCED on {local_arch} "
                        f"({ci_dir} on {'/'.join(archs)} in CI; {why})"
                    )
                not_reproduced += 1
        print(
            f"{cq.test[:32]:<32} {cq.query_index:>3}  {ci_at:<6}"
            f"{fmt_sec(cq.left):>8} {fmt_sec(cq.right):>8} {fmt_diff(cq.diff):>8} | "
            f"{local_str}  {verdict}"
        )
        split_arch |= print_arch_disagreement(cq)
    print()
    print(
        f"Summary: {confirmed} confirmed, "
        f"{not_reproduced} not reproduced, "
        f"{len(changed) - confirmed - not_reproduced} not measured"
        + (f" (of which {failed} from failed perf.py runs)" if failed else "")
        + (f" ({errored} errored on every server)" if errored else "")
        + (f", {unverifiable} without a verdict" if unverifiable else "")
    )
    if unreadable:
        print(
            f"INCOMPLETE: {len(unreadable)} shard report(s) could not be read "
            f"({describe_unreadable(unreadable)}). Queries those shards flagged "
            "are missing from this table, so it is not the whole comparison."
        )
    if split_arch:
        print(
            "'CI split' lines: CI flagged the query on both arches but in "
            "opposite directions. The table row carries one arch's numbers; "
            "the split line has all of them."
        )
    if errored:
        print(
            "A query marked ERRORED did not run at all — it is neither "
            "confirmed nor refuted. perf.py drops such a query and still "
            "exits 0, so the per-test stderr log is the only record."
        )
    if failed_tests:
        print(
            "WARNING: perf.py failed for "
            + ", ".join(f"{t} (exit {rc})" for t, rc in sorted(failed_tests.items()))
            + " — those queries were not validated, they did not 'fail to reproduce'."
        )
    if any(c.ci_unconfirmed for c in changed):
        print(
            "'*' in the CI@ column: CI flagged the query and then demoted it in "
            "its own confirmation rerun ('Unconfirmed Changes' in the report), "
            "so its numbers come from report.html and its CI verdict was "
            "already in doubt before this local rerun."
        )
    if not local_arch_measured:
        print(
            f"CI ran no {local_arch} shard for this commit, so every CI number "
            f"above is from another architecture. This rerun says whether the "
            f"change also shows on {local_arch}; it cannot confirm or refute "
            f"the arch CI measured."
        )
    print(
        f"CI@ column: which arch(es) CI flagged the query on. "
        f"'<arch>-only' means CI did NOT flag it on {local_arch} (the "
        f"CI old/new/Δ columns then reflect timings from the other arch, "
        f"so don't directly compare them to the local {local_arch} rerun)."
    )


# ---------------------------------------------------------------------------
# Dataset detection
# ---------------------------------------------------------------------------

DEFAULT_DATASET_PATHS = [
    Path("ci/tmp/perf_wd/db0"),
]


def detect_dataset(explicit: Optional[Path]) -> Optional[Path]:
    if explicit:
        if not explicit.is_dir():
            die(f"--db-path {explicit} does not exist or is not a directory")
        return explicit
    for cand in DEFAULT_DATASET_PATHS:
        if cand.is_dir() and (cand / "data").is_dir():
            return cand
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit", help="commit SHA from the PR perf check")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="path to the dataset directory (must contain data/, metadata/, "
        "etc. — same layout as ci/tmp/perf_wd/db0). "
        "Default: probe known locations.",
    )
    parser.add_argument(
        "--pr",
        type=int,
        default=None,
        help="PR number (override auto-detection via gh)",
    )
    parser.add_argument(
        "--reference-sha",
        default=None,
        help="left/baseline SHA (override report.html parsing)",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="working directory (default: tmp/double_check_perf in cwd)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=None,
        help="minimum measurements per query. Unset by default, like CI: "
        "perf.py's adaptive policy then decides the counts from its "
        "--min-runs/--tau precision stop. Passing a value only widens that "
        "policy, and changes the sampling -- and so the medians, the rerun "
        "precision and the verdict",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve commit/PR/SHA/changed queries and print plan; do not run",
    )
    parser.add_argument(
        "--populate",
        action="store_true",
        help="rebuild the hits datasets on each server separately (INSERT "
        "SELECT + OPTIMIZE FINAL), exactly as CI's populate_data_both does, "
        "so each side's parts are written by its own binary. Required to see "
        "write-path changes (serialization, sparse columns, statistics, mark "
        "format) -- with the default hardlinked dataset both sides read parts "
        "written by whatever binary produced the tarball, so such regressions "
        "come back NOT REPRODUCED. Costs a full rewrite of each affected hits "
        "table per side (hits_100m_single alone is ~21 GiB and tens of "
        "minutes) and gives up the hardlink disk saving for them.",
    )
    parser.add_argument(
        "--use-working-tree-tests",
        action="store_true",
        help="run this checkout's tests/performance and configs instead of the "
        "ones from the commit under test. Only for iterating on a local change "
        "to a test: query indices are positional, so a checkout that differs "
        "from the measured commit can silently rerun a different query",
    )
    parser.add_argument(
        "--no-cpu-pinning",
        action="store_true",
        help="do not pin the servers with taskset and do not cap max_threads "
        "(CI pins both servers to one hyperthread per physical core on x86_64; "
        "unpinning measures under noisier conditions than the report being "
        "checked)",
    )
    parser.add_argument(
        "--port-offset",
        type=int,
        default=0,
        help="add this to every port the script uses. The defaults mirror "
        "CI, where the left server sits on the standard ClickHouse ports; "
        "shift them when a local server already owns those. Does not affect "
        "what is measured.",
    )
    parser.add_argument(
        "--skip-wait-for-merges",
        action="store_true",
        help="don't wait for background merges to quiesce before running "
        "tests (only safe if datasets were already settled from a previous "
        "run)",
    )
    args = parser.parse_args()

    repo_root = Path.cwd()
    if not (repo_root / "tests/performance/scripts/perf.py").is_file():
        die("must be run from the root of a ClickHouse checkout")

    if args.port_offset:
        apply_port_offset(args.port_offset)
        log(f"port offset {args.port_offset}: left TCP {LEFT_TCP}, "
            f"right TCP {RIGHT_TCP}")

    perf_arch, build_type = detect_arch()
    log(f"machine arch: {perf_arch} ({build_type})")

    pr_sha = find_full_sha(args.commit)
    log(f"resolved commit: {pr_sha}")

    pr_number = args.pr or find_pr_for_commit(pr_sha)
    log(f"PR: #{pr_number}")

    shards = get_performance_shards(pr_number, pr_sha)
    if not shards:
        die(
            f"no Performance Comparison shards found for PR #{pr_number} sha {pr_sha}"
        )
    # A skipped shard published nothing, so its synthesized report URL would
    # just 403. Treating that as "CI found no changes" is the difference
    # between "the comparison ran and was clean" and "the comparison never
    # ran" — the tool exists to tell CI's verdict, so it must not invent one.
    not_run = [s for s in shards if s.status.upper() in NOT_RUN_STATUSES]
    shards = [s for s in shards if s.status.upper() not in NOT_RUN_STATUSES]
    if not shards:
        statuses = ", ".join(
            f"{st} x{n}" for st, n in sorted(
                Counter(s.status for s in not_run).items()
            )
        )
        die(
            f"all {len(not_run)} Performance Comparison shard(s) for "
            f"{pr_sha[:12]} are not run ({statuses}) — CI never measured this "
            "commit, so there is nothing to double-check. This is normal for a "
            "PR that does not touch anything the perf check runs on. Point the "
            "skill at a commit whose perf check actually ran."
        )
    if not_run:
        log(f"ignoring {len(not_run)} shard(s) that did not run")

    # See SUPPORTED_BASELINE: a `release_base` shard compares a different pair
    # of binaries over a different tests tree, and merging its rows into the
    # master_head rerun -- they share the (test, query_index) key -- would
    # adjudicate them against a binary CI never used. Only the master workflow
    # schedules that flavour today and its reports live under REFs/, which this
    # tool does not read, so this is a guard against the day that changes.
    unsupported = [s for s in shards if s.baseline != SUPPORTED_BASELINE]
    if unsupported:
        kinds = ", ".join(
            f"{k} x{n}" for k, n in sorted(Counter(s.baseline for s in unsupported).items())
        )
        die(
            f"report contains {len(unsupported)} Performance Comparison "
            f"shard(s) with an unsupported baseline ({kinds}); only "
            f"{SUPPORTED_BASELINE} can be reproduced locally. Such a shard "
            "measures against a release build with that release's "
            "tests/performance checkout, so its query indices and its "
            "reference binary are not the ones this tool would rerun, and the "
            "reference-SHA lookup cannot tell the two baselines apart. "
            "Rerunning would silently adjudicate those queries against the "
            "wrong pair."
        )

    arch_shards = [s for s in shards if s.arch == perf_arch]
    other_arch_shards = [s for s in shards if s.arch != perf_arch]
    # The play.clickhouse.com lookups are keyed by architecture, so they have
    # to ask about an arch CI actually measured. What they return is a master
    # *commit*, not a binary, and every master build publishes both arches --
    # so the local-arch binaries for that commit exist even when CI ran only
    # the other arch. Nothing else here is arch-bound: the download path is
    # built from the local build type, and the report already labels rows CI
    # flagged elsewhere. Rerunning them is the whole point of the cross-arch
    # rule, and it does not stop applying when the local arch happens to have
    # no shards at all -- a perf check runs on AMD only for a PR labeled
    # `pr-performance`, so an ARM-only report is the common case, not an edge.
    reference_arch = perf_arch
    if not arch_shards:
        reference_arch = sorted({s.arch for s in other_arch_shards})[0]
        log(
            f"WARNING: CI ran no {perf_arch} perf shard for this commit "
            f"(only {sorted({s.arch for s in shards})}). Every flagged query "
            f"is rerun on {perf_arch} anyway, but the CI old/new/Δ columns are "
            f"{reference_arch} timings: NOT REPRODUCED then means "
            f"'{perf_arch} does not show it', not 'CI was wrong'. The "
            f"reference SHA comes from the {reference_arch} run."
        )
    else:
        log(
            f"found {len(arch_shards)} {perf_arch} perf shard(s): "
            f"{[(s.baseline, f'{s.shard_num}/{s.total_shards}') for s in arch_shards]}"
        )

    # Collect changes from every arch. We re-run them all locally regardless
    # of which arch CI flagged them on, because:
    #   - a query flagged only on ARM might still drift on AMD silently,
    #     and the user wants to know
    #   - cross-arch noise is informative either way ("local AMD rerun
    #     can't reproduce the ARM regression" is a meaningful result)
    # When the same (test, query_index) is flagged on more than one arch,
    # we keep one row per query and remember every arch it was flagged on.
    local_changed, local_read, local_unresolved, local_unreadable = (
        find_changed_queries(arch_shards)
    )
    if other_arch_shards:
        other_changed, other_read, other_unresolved, other_unreadable = (
            find_changed_queries(other_arch_shards)
        )
    else:
        other_changed, other_read, other_unresolved, other_unreadable = [], 0, [], []
    unresolved = local_unresolved + other_unresolved
    unreadable = local_unreadable + other_unreadable
    if local_read + other_read == 0:
        die(
            f"none of the {len(shards)} Performance Comparison shard(s) for "
            f"{pr_sha[:12]} could be read (report.html unavailable — artifacts "
            "expired, or the run did not publish them). Without a report there "
            "is no way to tell 'CI flagged nothing' from 'no data', so no "
            "verdict is possible."
        )
    if unreadable:
        log(
            f"WARNING: {len(unreadable)} shard report(s) could not be read "
            f"({describe_unreadable(unreadable)}) — whatever those shards "
            "flagged is invisible here, so this double-check covers only part "
            "of the comparison"
        )
    log(f"changed queries flagged by CI on {perf_arch}: {len(local_changed)}")
    if other_changed:
        log(
            f"changed queries flagged by CI on other arch(es) "
            f"({sorted({c.arch for c in other_changed})}): {len(other_changed)} "
            f"— will still be rerun locally with a note"
        )

    # Dedup: prefer the row from the local arch (its left/right/diff are
    # directly comparable to our local rerun); fall back to whatever
    # other-arch row we saw. Track every arch that flagged it.
    by_key: dict[tuple[str, int], ChangedQuery] = {}
    flagged_on: dict[tuple[str, int], list[str]] = defaultdict(list)
    ci_by_arch: dict[tuple[str, int], dict[str, dict]] = defaultdict(dict)
    for cq in local_changed:
        key = (cq.test, cq.query_index)
        by_key[key] = cq
        if cq.arch not in flagged_on[key]:
            flagged_on[key].append(cq.arch)
    for cq in other_changed:
        key = (cq.test, cq.query_index)
        if key not in by_key:
            by_key[key] = cq
        if cq.arch not in flagged_on[key]:
            flagged_on[key].append(cq.arch)
    # Only one row per query survives the dedup, so keep each arch's numbers
    # separately: the report has to be able to say that CI called the same
    # query slower on one arch and faster on the other.
    for cq in local_changed + other_changed:
        ci_by_arch[(cq.test, cq.query_index)].setdefault(
            cq.arch,
            {
                "left": cq.left,
                "right": cq.right,
                "diff": cq.diff,
                "direction": cq.direction,
                "shard_num": cq.shard_num,
            },
        )
    for key, cq in by_key.items():
        cq.flagged_on = sorted(flagged_on[key])
        cq.ci_by_arch = dict(sorted(ci_by_arch[key].items()))

    changed = list(by_key.values())
    if unresolved:
        log(
            f"WARNING: {len(unresolved)} query(ies) flagged by CI could not be "
            "read from either the TSV or report.html: "
            + ", ".join(f"{t} #{q} ({a}/{n})" for a, n, t, q in unresolved)
        )
    if not changed and unresolved:
        die(
            f"CI flagged {len(unresolved)} query(ies) but none of them could be "
            "read, so there is nothing to rerun and no basis for calling the "
            "comparison clean."
        )
    if not changed:
        if unreadable:
            die(
                f"CI flagged no 'Changes in Performance' in the "
                f"{local_read + other_read} shard report(s) that could be read, "
                f"but {len(unreadable)} shard report(s) could not be read at "
                f"all ({describe_unreadable(unreadable)}). Such a shard "
                "contributes no changed queries, exactly like a shard that had "
                "none, so calling the comparison clean here would be a claim "
                "about the part of it that was never inspected."
            )
        log(
            f"CI flagged no 'Changes in Performance' in the "
            f"{local_read + other_read} shard report(s) read — the comparison "
            "ran and was clean, nothing to double-check"
        )
        return 0
    log(f"unique queries to double-check: {len(changed)}")

    # Group changed queries by test
    by_test: dict[str, list[int]] = defaultdict(list)
    for cq in changed:
        if cq.query_index not in by_test[cq.test]:
            by_test[cq.test].append(cq.query_index)

    log(f"affected test XMLs: {len(by_test)}")
    for test, qs in sorted(by_test.items()):
        log(f"  {test}: q#{sorted(qs)}")

    # Set up working directory
    work_dir = args.work_dir or (repo_root / "tmp/double_check_perf")
    work_dir.mkdir(parents=True, exist_ok=True)

    # Everything below reads tests and configs from the commit under test, not
    # from the working tree.
    if args.use_working_tree_tests:
        log("WARNING: using this checkout's tests/performance, which may not "
            "match the commit CI measured — query numbering is positional")
        perf_root = repo_root
    else:
        perf_root = materialize_perf_tree(repo_root, pr_number, pr_sha, work_dir)

    # Inspect XMLs to figure out which external datasets are actually needed.
    # Self-contained tests (tables created via <create_query> / <fill_query>
    # filled from numbers/generateRandom) need none. Reporting this up-front
    # lets the caller know whether the 50 GB bootstrap is required at all,
    # or only a single tarball.
    test_texts = {
        t: relevant_test_text(
            perf_root, perf_root / "tests/performance" / f"{t}.xml", sorted(qs)
        )
        for t, qs in by_test.items()
        if (perf_root / "tests/performance" / f"{t}.xml").is_file()
    }
    needed_datasets = scan_external_datasets(test_texts)
    if needed_datasets:
        log("external datasets referenced by affected XMLs:")
        for name, xmls in sorted(needed_datasets.items()):
            log(f"  {name}  <- {', '.join(sorted(set(xmls)))}")
        urls = sorted({EXTERNAL_DATASETS[n] for n in needed_datasets})
        log("download URL(s) needed: " + " ".join(urls))
    else:
        log(
            "no external datasets referenced — all affected tests create "
            "their own tables; no preloaded data required"
        )

    # Under --populate, only rebuild the hits tables the affected XMLs
    # actually touch: rewriting hits_100m_single for a test that never reads
    # it is tens of minutes for nothing. CI has no such luxury (it populates
    # before knowing which tests run) but we already scanned the XMLs.
    populate_tables: list[str] = []
    if args.populate:
        wanted = {
            "hits_10m_single": "default.hits_10m_single",
            "hits_100m_single": "default.hits_100m_single",
            "hits": "test.hits",
            "hits_v1": "test.hits",
            "test.hits": "test.hits",
        }
        for name in needed_datasets:
            table = wanted.get(name)
            if table and table not in populate_tables:
                populate_tables.append(table)
        if populate_tables:
            log(f"--populate: will rebuild on each side: {', '.join(populate_tables)}")
        else:
            log(
                "--populate: no hits dataset referenced by the affected XMLs; "
                "nothing to rebuild (tests that build their own tables already "
                "write them with each side's own binary)"
            )

    # Queries CI demoted come from report.html, which does not carry the
    # per-query threshold CI applied. Recover it the way compare.sh builds it,
    # anchoring the historical window on the day the run happened.
    # Only rows whose numbers came from report.html: a shard old enough to
    # predate the changed_threshold column also has no threshold, but CI never
    # exported one for it either, so the documented 0.15 floor applies rather
    # than a reconstruction or a refusal to judge.
    demoted = [
        cq for cq in changed
        if cq.numbers_from_html and cq.changed_threshold is None
    ]
    if demoted:
        run_day = fetch_run_day(pr_number, pr_sha, reference_arch)
        historical = fetch_historical_thresholds(run_day) if run_day else None
        if historical is None:
            log(
                f"WARNING: could not recover the per-query threshold for "
                f"{len(demoted)} demoted query(ies); they will be reported "
                "without a verdict"
            )
        for cq in demoted:
            xml = perf_root / "tests/performance" / f"{cq.test}.xml"
            if historical is None:
                cq.threshold_unknown = True
                continue
            display_name = query_display_name_from_tree(
                perf_root, xml, cq.query_index
            )
            cq.changed_threshold = ci_changed_threshold(
                historical.get((cq.test, cq.query_index, display_name), 0.0),
                test_report_threshold(xml),
            )
            log(
                f"  {cq.test} #{cq.query_index}: CI threshold "
                f"{cq.changed_threshold:.0%} (recovered from the run's "
                f"historical window ending {run_day})"
            )

    # Find the reference SHA. A dry run only prints it, so it must not be the
    # thing that makes the planning path require `clickhouse client`.
    ref_sha = args.reference_sha or fetch_reference_sha(
        pr_number, pr_sha, reference_arch, required=not args.dry_run
    )
    if ref_sha:
        log(f"reference SHA: {ref_sha}")
    else:
        log(
            "reference SHA: unresolved — a real run needs it, so install "
            "clickhouse client or pass --reference-sha before rerunning "
            "without --dry-run"
        )

    if args.dry_run:
        log("--dry-run: stopping before downloads")
        return 0

    # Dataset check. If no external datasets are referenced by any affected
    # XML, we don't need preloaded data at all — auto-create an empty db0 so
    # the rest of the pipeline (which expects a database directory) has
    # something to hardlink from.
    db_source = detect_dataset(args.db_path)
    if db_source is None:
        if not needed_datasets:
            db_source = repo_root / "ci/tmp/perf_wd/db0"
            (db_source / "data" / "default").mkdir(parents=True, exist_ok=True)
            (db_source / "data" / "datasets").mkdir(parents=True, exist_ok=True)
            (db_source / "metadata").mkdir(parents=True, exist_ok=True)
            log(
                f"no external datasets needed; created empty {db_source} "
                "for the framework to hardlink"
            )
        else:
            urls = sorted({EXTERNAL_DATASETS[n] for n in needed_datasets})
            die(
                "external datasets are required by the affected XMLs but no "
                "dataset directory was found. Either pass --db-path pointing "
                "at a dir with these tables loaded, or bootstrap by extracting "
                "ONLY the needed tarballs into ci/tmp/perf_wd/db0:\n  "
                + "\n  ".join(urls)
                + "\nFull bootstrap of all 6 perf tarballs would be ~50 GB; "
                "the list above is the actual subset needed for this run."
            )
    log(f"dataset: {db_source}")

    # Locate test XML files (translate stem -> xml)
    test_files: dict[str, str] = {}
    for test in by_test:
        xml_path = perf_root / "tests/performance" / f"{test}.xml"
        if not xml_path.is_file():
            log(f"WARNING: {xml_path} not found — skipping test {test}")
            continue
        test_files[test] = f"{test}.xml"

    exit_code = 0

    require_perf_dependencies()

    # Download binaries
    left_bin, right_bin = download_binaries(
        repo_root, work_dir, pr_number, pr_sha, ref_sha, build_type
    )
    log(f"left  binary: {left_bin}")
    log(f"right binary: {right_bin}")

    # Prepare top-level domains
    tld_src = perf_root / "tests/config/top_level_domains"
    tld_dst = work_dir / "top_level_domains"
    if tld_dst.exists():
        shutil.rmtree(tld_dst)
    if tld_src.is_dir():
        shutil.copytree(tld_src, tld_dst, symlinks=False)
    else:
        tld_dst.mkdir(parents=True, exist_ok=True)

    # Prepare per-side dirs. We need left/{clickhouse-server,config} ready
    # before prepare_dataset, because the preconfig step (which does the
    # SQL CREATE DATABASE/RENAME TABLE) uses the left binary.
    for side, binary in (("left", left_bin), ("right", right_bin)):
        side_dir = work_dir / side
        side_dir.mkdir(parents=True, exist_ok=True)
        link_clickhouse_tools(side_dir)
        prepare_configs(perf_root, side_dir)

    prepare_dataset(db_source, left_bin, work_dir / "left" / "config",
                    tld_dst, work_dir, create_test_hits=not args.populate)

    for side in ("left", "right"):
        side_dir = work_dir / side
        hardlink_db(db_source, side_dir / "db")
        # The embedded Keeper's state has to be dropped together with the data
        # it describes. The work dir is shared between runs, and
        # `alter_select.xml` creates a
        # `ReplicatedMergeTree('/tables/{database}', '{table}')`: against a
        # fresh db but the previous run's znodes, its create_query fails with
        # REPLICA_ALREADY_EXISTS and the whole test goes unmeasured.
        shutil.rmtree(side_dir / "coordination", ignore_errors=True)
        # After the hardlink copy: hardlink_db wipes the destination.
        seed_user_files(perf_root, side_dir / "db")

    # Mirror CI's CPU pinning: both servers on one hyperthread per physical
    # core, with max_threads capped to that set. Skipped by --no-cpu-pinning
    # and on anything but Linux x86_64 (CI does not pin there either).
    cpu_list: Optional[str] = None
    if args.no_cpu_pinning:
        log("CPU pinning disabled by --no-cpu-pinning")
    elif cpu_pinning_enabled(perf_arch):
        cpu_list = get_physical_core_cpu_list()
        max_threads = len(cpu_list.split(","))
        for side in ("left", "right"):
            write_max_threads_override(work_dir / side, max_threads)
        log(f"pinning both servers to cpus {cpu_list} (max_threads={max_threads})")
    else:
        log(f"CPU pinning not applicable on {perf_arch}/{os.uname().sysname}")

    # Start servers
    ensure_ports_free({
        LEFT_TCP: "left TCP", LEFT_HTTP: "left HTTP",
        LEFT_KEEPER_TCP: "left keeper", LEFT_KEEPER_RAFT: "left keeper raft",
        LEFT_INTERSERVER: "left interserver",
        RIGHT_TCP: "right TCP", RIGHT_HTTP: "right HTTP",
        RIGHT_KEEPER_TCP: "right keeper", RIGHT_KEEPER_RAFT: "right keeper raft",
        RIGHT_INTERSERVER: "right interserver",
    })
    left_h = start_server(
        work_dir / "left", "left",
        LEFT_TCP, LEFT_HTTP, LEFT_KEEPER_TCP, LEFT_KEEPER_RAFT, LEFT_INTERSERVER,
        tld_dst, cpu_list,
    )
    right_h = start_server(
        work_dir / "right", "right",
        RIGHT_TCP, RIGHT_HTTP, RIGHT_KEEPER_TCP, RIGHT_KEEPER_RAFT,
        RIGHT_INTERSERVER, tld_dst, cpu_list,
    )

    try:
        if not wait_server_ready(left_h):
            die(server_start_failure(left_h))
        if not wait_server_ready(right_h):
            die(server_start_failure(right_h))
        log("both servers ready")

        if populate_tables:
            populate_data_both([left_h, right_h], populate_tables)

        # If the dataset directory was just freshly populated, ClickHouse
        # will be busy consolidating parts that came in at various merge
        # levels. Measuring before those settle yields unstable numbers.
        if not args.skip_wait_for_merges:
            wait_for_merges([left_h, right_h])

        # Run perf.py per test
        side_dbs = [work_dir / side / "db" for side in ("left", "right")]
        failed_tests: dict[str, int] = {}
        errored_tests: set[str] = set()
        for test, qs in sorted(by_test.items()):
            if test not in test_files:
                continue
            _, rc = run_perf_test(
                perf_root, work_dir, test_files[test], sorted(qs), args.runs
            )
            if rc != 0:
                failed_tests[test] = rc
            else:
                # A query that failed on every server is skipped by perf.py
                # with a zero exit and nothing but a stderr traceback, which
                # would otherwise surface as the benign-looking "no local
                # data". Anything on stderr from an otherwise-clean run means
                # at least one query did not run.
                err_log = work_dir / "raw" / f"{test}-err.log"
                if err_log.is_file() and err_log.stat().st_size > 0:
                    errored_tests.add(test)
                    log(f"ERROR: perf.py reported query errors for {test} "
                        f"but exited 0; see {err_log}")
            cleanup_user_files(side_dbs)

        # Collect results. stat_threshold is recomputed from the per-run
        # samples with perf.py's own function, the way compare.sh recomputes it
        # for its confirmation rerun.
        stat_threshold_fn = load_stat_threshold(perf_root)
        local_results: dict[tuple[str, int], dict] = {}
        for test in by_test:
            raw = work_dir / "raw" / f"{test}-raw.tsv"
            diffs = parse_perf_diffs(raw)
            runs = parse_perf_runs(raw)
            for qi, d in diffs.items():
                sides = runs.get(qi, {})
                d["stat_threshold"] = (
                    stat_threshold_fn(sides[0], sides[1])
                    if 0 in sides and 1 in sides
                    else None
                )
                local_results[(test, qi)] = d

        print_report(changed, local_results, perf_arch, failed_tests,
                     unreadable, local_arch_measured=bool(arch_shards),
                     errored_tests=errored_tests)
        # JSON dump for downstream use
        json_path = work_dir / "result.json"
        json_path.write_text(
            json.dumps(
                {
                    "commit": pr_sha,
                    "pr": pr_number,
                    "arch": perf_arch,
                    "reference_sha": ref_sha,
                    "populated_tables": populate_tables,
                    "changed": [cq.__dict__ for cq in changed],
                    "failed_tests": failed_tests,
                    "errored_tests": sorted(errored_tests),
                    "unreadable_shards": [
                        {"arch": a, "shard": n, "of": t, "error": e}
                        for a, n, t, e in unreadable
                    ],
                    "local": {
                        f"{k[0]}#{k[1]}": v for k, v in local_results.items()
                    },
                },
                indent=2,
            )
        )
        log(f"wrote {json_path}")
        if failed_tests or errored_tests or unreadable:
            # An unreadable shard leaves part of the comparison uninspected;
            # a zero exit would read as "all of CI's findings were checked".
            exit_code = 1
    finally:
        stop_server(right_h)
        stop_server(left_h)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
