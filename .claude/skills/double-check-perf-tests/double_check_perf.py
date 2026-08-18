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
     reading from a hardlinked copy of the dataset directory) configured
     exactly like CI's performance-comparison job.
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
import json
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

REPO = "ClickHouse/ClickHouse"

# Public S3 layout used by CI:
#   PRs:     clickhouse-builds.s3.amazonaws.com/PRs/<pr>/<sha>/<build_type>/clickhouse
#   master:  clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/<sha>/<build_type>/clickhouse
BUILDS_BUCKET_PR = "https://clickhouse-builds.s3.amazonaws.com"
BUILDS_BUCKET_MASTER = "https://clickhouse-builds.s3.us-east-1.amazonaws.com"
REPORTS_BUCKET = "https://s3.amazonaws.com/clickhouse-test-reports"

# Ports — must match performance_tests.py so config files we copy work.
LEFT_TCP = 9001
LEFT_KEEPER_TCP = 9181
LEFT_KEEPER_RAFT = 9234
LEFT_INTERSERVER = 9009

RIGHT_TCP = 19001
RIGHT_KEEPER_TCP = 19181
RIGHT_KEEPER_RAFT = 19234
RIGHT_INTERSERVER = 19009


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


def find_pr_for_commit(sha: str) -> int:
    """Use gh to find the PR number that contains this commit."""
    try:
        out = subprocess.check_output(
            [
                "gh",
                "api",
                f"repos/{REPO}/commits/{sha}/pulls",
                "--jq",
                ".[] | {number: .number, state: .state}",
            ],
            text=True,
        )
    except subprocess.CalledProcessError as e:
        die(f"gh failed to resolve PR for commit {sha}: {e}")
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
    try:
        out = subprocess.check_output(
            ["gh", "api", f"repos/{REPO}/commits/{sha}", "--jq", ".sha"],
            text=True,
        ).strip()
    except subprocess.CalledProcessError as e:
        die(f"gh failed to resolve SHA {sha}: {e}")
    return out


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


def get_performance_shards(pr_number: int, sha: str) -> list[PerfShard]:
    pr_json_url = f"{REPORTS_BUCKET}/PRs/{pr_number}/{sha}/result_pr.json"
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
                    base_dir = f"{REPORTS_BUCKET}/PRs/{pr_number}/{sha}/{dir_name}"

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
    # Filled in later, after we've collected all changes across arches:
    # which arches CI flagged this same (test, query_index) on. Useful so
    # the report can say "flagged on ARM only" vs "flagged on both".
    flagged_on: list[str] = field(default_factory=list)


def parse_query_metrics_tsv(text: str, metric: str = "client_time"):
    """Yield rows from all-query-metrics.tsv.

    Layout (compare.sh report()):
        metric_name, left, right, diff, times_change, stat_threshold,
        test, query_index, query_display_name
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
        }


def parse_changes_in_performance(html: str) -> set[tuple[str, int]]:
    """Extract (test, query_index) for every row CI categorized as a
    "Change in Performance". The HTML wraps each row with an id like
    ``changes-in-performance.<test>.<query_index>``."""
    m = re.search(r"id=changes-in-performance.*?</table>", html, re.DOTALL)
    if not m:
        return set()
    out: set[tuple[str, int]] = set()
    for test, qi in re.findall(
        r"<tr id=changes-in-performance\.([^>.]+?)\.(\d+)>", m.group(0)
    ):
        out.add((test, int(qi)))
    return out


def find_changed_queries(shards: list[PerfShard]) -> list[ChangedQuery]:
    """Identify rows the CI report flags under "Changes in Performance".

    compare.sh computes the predicate at report time using per-test thresholds
    (default ``changed_threshold=0.1`` raised by historical 99th-percentile
    diff and the ``<report_threshold>`` in the XML). We don't have those
    inputs outside CI, so instead of re-implementing the predicate we let
    CI tell us what counts: fetch each shard's ``report.html``, extract the
    ``id=changes-in-performance.<test>.<query_index>`` table, then pull the
    timing numbers for those tuples from ``all-query-metrics.tsv``. This is
    exactly the set the user sees in the report.
    """
    changed: list[ChangedQuery] = []
    for s in shards:
        try:
            html = http_get(f"{s.base_dir_url}/report.html")
        except RuntimeError as e:
            log(f"skipping shard {s.arch}/{s.shard_num} report.html: {e}")
            continue
        flagged = parse_changes_in_performance(html)
        if not flagged:
            continue

        try:
            tsv = http_get(s.tsv_url)
        except RuntimeError as e:
            log(f"skipping shard {s.arch}/{s.shard_num} TSV: {e}")
            continue
        # Index TSV rows so we can fetch timing numbers for each flagged
        # (test, query_index) tuple. There's one row per metric, we only
        # want client_time.
        timings: dict[tuple[str, int], dict] = {}
        for row in parse_query_metrics_tsv(tsv):
            timings[(row["test"], row["query_index"])] = row

        for test, qi in sorted(flagged):
            row = timings.get((test, qi))
            if row is None:
                log(
                    f"  WARNING: report.html flagged {test} #{qi} but no "
                    f"client_time row in shard {s.arch}/{s.shard_num} TSV"
                )
                continue
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
                )
            )
    return changed


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


def scan_external_datasets(xml_paths: list[Path]) -> dict[str, list[str]]:
    """Return {dataset_name: [xml_basename, ...]} for every external dataset
    referenced by at least one of the given XMLs.

    We treat the XML as a body of text plus any sibling SQL files pulled in
    via ``<query file="...">``. That catches benchmarks like ``tpcds.xml``
    and ``tpch.xml`` whose XML only says ``USE {table}`` but whose actual
    query bodies live in ``../benchmarks/tpc-ds/queries/*.sql`` and reference
    ``store_sales`` / ``lineitem`` / etc. without a database qualifier — so
    we also flag the dataset when an XML does ``USE tpcds`` / ``USE tpch``
    / etc.
    """
    found: dict[str, list[str]] = defaultdict(list)
    db_aliases = {
        "tpcds": "tpcds.store_sales",   # any tpcds.* entry maps to the same tarball
        "tpch":  "tpch.lineitem",
    }
    for path in xml_paths:
        try:
            text = path.read_text()
        except OSError:
            continue
        # Pull in any referenced SQL files so unqualified table names inside
        # them surface (e.g. tpcds queries use bare 'store_sales' etc.).
        for ref in re.findall(r'<query\s+file="([^"]+)"', text):
            sibling = (path.parent / ref).resolve()
            if sibling.is_file():
                try:
                    text += "\n" + sibling.read_text()
                except OSError:
                    pass

        for name in EXTERNAL_DATASETS:
            if re.search(rf"(?<![\w.]){re.escape(name)}(?![\w])", text):
                found[name].append(path.name)

        # Heuristic for benchmarks whose tables aren't database-qualified in
        # the SQL bodies. Catch both forms:
        #   - direct: "USE tpcds" / "FROM tpcds" / etc.
        #   - via substitution: "USE {table}" with <value>tpcds</value> in
        #     a substitution block (this is how tpcds.xml / tpch.xml work).
        for db_word, canonical in db_aliases.items():
            if re.search(rf"\b(?:USE|FROM)\s+{db_word}\b", text, re.IGNORECASE):
                found[canonical].append(path.name)
                continue
            if re.search(rf"<value>\s*{db_word}\s*</value>", text):
                found[canonical].append(path.name)
    return dict(found)


# ---------------------------------------------------------------------------
# Reference SHA discovery (the "left" binary)
# ---------------------------------------------------------------------------


def fetch_reference_sha(pr_sha: str, perf_arch: str) -> str:
    """Find the reference (left/baseline) git SHA used in the CI run.

    The CI uploads each perf-test row to ``query_metrics_v2`` on
    play.clickhouse.com, which has both ``new_sha`` (the PR commit) and
    ``old_sha`` (the reference binary's git hash). That's the most reliable
    source: ``report.html`` shows ``clickhouse --version`` for the reference,
    which for official builds doesn't include the SHA, and ``left-commit.txt``
    inside ``logs.tar.zst`` has the same problem.
    """
    query = (
        "SELECT DISTINCT old_sha FROM query_metrics_v2 "
        f"WHERE new_sha = '{pr_sha}' AND arch = '{perf_arch}' LIMIT 1 FORMAT TSV"
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
        die(
            "clickhouse client not found on PATH — required to query "
            "play.clickhouse.com for the reference SHA. Install it or pass "
            "--reference-sha explicitly."
        )
    except subprocess.CalledProcessError as e:
        die(
            f"play.clickhouse.com query failed: {e.stderr.strip()}; "
            "pass --reference-sha explicitly"
        )
    ref_sha = out.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", ref_sha):
        die(
            f"play.clickhouse.com returned no row for new_sha={pr_sha} "
            f"arch={perf_arch} (got: {ref_sha!r}); pass --reference-sha "
            "explicitly"
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

    # Right binary (patched / PR)
    right_url = f"{BUILDS_BUCKET_PR}/PRs/{pr_number}/{pr_sha}/{build_type}/clickhouse"
    # Fallback for master-tip commits (no PR): same path under REFs/<branch>/<sha>
    candidate_right_urls = [right_url]
    candidate_right_urls.append(
        f"{BUILDS_BUCKET_MASTER}/REFs/master/{pr_sha}/{build_type}/clickhouse"
    )
    if not right_bin.is_file():
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

    # Left binary (reference / baseline) — always built off master
    left_url = f"{BUILDS_BUCKET_MASTER}/REFs/master/{ref_sha}/{build_type}/clickhouse"
    if not left_bin.is_file():
        if not http_head_ok(left_url):
            die(f"reference binary not found at {left_url}")
        download(left_url, left_bin)
        left_bin.chmod(0o755)

    return left_bin, right_bin


# ---------------------------------------------------------------------------
# Server setup (mirrors performance_tests.py)
# ---------------------------------------------------------------------------


def link_clickhouse_tools(side_dir: Path) -> None:
    """Create the clickhouse-{server,client,local,keeper} symlinks next to the
    main binary, the way CHServer expects."""
    binary = side_dir / "clickhouse"
    for name in ("clickhouse-server", "clickhouse-client", "clickhouse-local", "clickhouse-keeper"):
        target = side_dir / name
        if target.is_symlink() or target.exists():
            target.unlink()
        target.symlink_to(binary.name)


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


def prepare_dataset(db_source: Path, binary_for_preconfig: Path,
                    config_dir: Path, top_level_domains: Path,
                    work_dir: Path) -> None:
    """Ensure the shared dataset directory has the bookkeeping the perf
    framework expects.

    - ``default`` and ``datasets`` are Ordinary databases (always needed).
    - If ``datasets.hits_v1`` exists and ``test.hits`` doesn't, do
      ``CREATE DATABASE test; RENAME TABLE datasets.hits_v1 TO test.hits``
      via a temporary clickhouse-server pointed at ``db_source``.

    *Why a temp server and not a filesystem rename?* Filesystem moves of
    metadata files leave ClickHouse's per-table CREATE-query state in an
    inconsistent enough form that loading other databases (specifically
    ``tpcds`` in our case) hits a NULL pointer in
    ``DatabaseOrdinary::getConvertToReplicatedFlagPath`` when running
    against the hardlinked copy. SQL ``RENAME TABLE`` performs the
    transition cleanly. This is what ``ci/jobs/performance_tests.py``
    does in its preconfig step.

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
        (db_source / "data" / "datasets" / "hits_v1").is_dir()
        and not (db_source / "data" / "test" / "hits").exists()
    )
    if needs_rename:
        log(
            "running preconfig server on db0 to create test.hits "
            "(SQL: CREATE DATABASE test; RENAME TABLE datasets.hits_v1 TO test.hits)"
        )
        coord = work_dir / "coordination0"
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
                "--tcp_port", str(LEFT_TCP),
            ]
            proc = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT)
        client = binary_for_preconfig.parent / "clickhouse-client"
        try:
            # Wait for the preconfig server to come up
            ready = False
            for _ in range(30):
                try:
                    out = subprocess.check_output(
                        [str(client), "--port", str(LEFT_TCP), "--query", "select 1"],
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
            for sql in (
                "CREATE DATABASE IF NOT EXISTS test",
                "RENAME TABLE datasets.hits_v1 TO test.hits",
            ):
                subprocess.run(
                    [str(client), "--port", str(LEFT_TCP), "--query", sql],
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
    keeper_port: int
    raft_port: int
    interserver_port: int
    proc: Optional[subprocess.Popen] = field(default=None)
    log_path: Path = field(default=Path("/dev/null"))


def start_server(
    side_dir: Path,
    name: str,
    port: int,
    keeper_port: int,
    raft_port: int,
    interserver_port: int,
    top_level_domains: Path,
) -> ServerHandle:
    log_path = side_dir / "server.log"
    log_fh = open(log_path, "w")
    cmd = [
        str(side_dir / "clickhouse-server"),
        "--config-file=" + str(side_dir / "config" / "config.xml"),
        "--",
        "--path", str(side_dir / "db"),
        "--user_files_path", str(side_dir / "db" / "user_files"),
        "--top_level_domains_path", str(top_level_domains),
        "--tcp_port", str(port),
        "--keeper_server.tcp_port", str(keeper_port),
        "--keeper_server.raft_configuration.server.port", str(raft_port),
        "--keeper_server.storage_path", str(side_dir / "coordination"),
        "--zookeeper.node.port", str(keeper_port),
        "--interserver_http_port", str(interserver_port),
    ]
    log(f"starting {name} server on TCP {port}: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd, stdout=log_fh, stderr=subprocess.STDOUT)
    handle = ServerHandle(
        name=name,
        side_dir=side_dir,
        port=port,
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
    runs: int,
) -> Path:
    """Run perf.py for one XML, restricted to the given query indices.

    Returns the path to the raw TSV output produced by perf.py."""
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
        "--runs", str(runs),
        "--profile-seconds", "0",
        "--queries-to-run", *[str(i) for i in query_indices],
    ]
    log(f"running perf.py on {test_xml} queries={query_indices}: {' '.join(cmd[:9])} ...")
    with open(out_path, "w") as out_fh, open(err_path, "w") as err_fh:
        subprocess.run(cmd, stdout=out_fh, stderr=err_fh, check=False)
    return out_path


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

        if local is None:
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
            confirmed_here = same_direction and abs(l_rel) > 0.10
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
                if local_arch in archs:
                    verdict = f"NOT REPRODUCED ({ci_dir} in CI)"
                else:
                    verdict = (
                        f"NOT REPRODUCED on {local_arch} "
                        f"({ci_dir} on {'/'.join(archs)} in CI)"
                    )
                not_reproduced += 1
        print(
            f"{cq.test[:32]:<32} {cq.query_index:>3}  {ci_at:<6}"
            f"{fmt_sec(cq.left):>8} {fmt_sec(cq.right):>8} {fmt_diff(cq.diff):>8} | "
            f"{local_str}  {verdict}"
        )
    print()
    print(
        f"Summary: {confirmed} confirmed, "
        f"{not_reproduced} not reproduced, "
        f"{len(changed) - confirmed - not_reproduced} not measured"
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
        default=7,
        help="number of measurements per query (default 7, same as CI)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve commit/PR/SHA/changed queries and print plan; do not run",
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
    arch_shards = [s for s in shards if s.arch == perf_arch]
    other_arch_shards = [s for s in shards if s.arch != perf_arch]
    if not arch_shards:
        die(
            f"no Performance Comparison shards for arch={perf_arch}; "
            f"available archs: {sorted({s.arch for s in shards})}"
        )
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
    local_changed = find_changed_queries(arch_shards)
    other_changed = find_changed_queries(other_arch_shards) if other_arch_shards else []
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
    for key, cq in by_key.items():
        cq.flagged_on = sorted(flagged_on[key])

    changed = list(by_key.values())
    if not changed:
        log("no queries categorized as 'Changes in Performance' on any arch — nothing to do")
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

    # Inspect XMLs to figure out which external datasets are actually needed.
    # Self-contained tests (tables created via <create_query> / <fill_query>
    # filled from numbers/generateRandom) need none. Reporting this up-front
    # lets the caller know whether the 50 GB bootstrap is required at all,
    # or only a single tarball.
    xml_paths = [
        repo_root / "tests/performance" / f"{t}.xml"
        for t in by_test
        if (repo_root / "tests/performance" / f"{t}.xml").is_file()
    ]
    needed_datasets = scan_external_datasets(xml_paths)
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

    # Find the reference SHA
    ref_sha = args.reference_sha or fetch_reference_sha(pr_sha, perf_arch)
    log(f"reference SHA: {ref_sha}")

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
        xml_path = repo_root / "tests/performance" / f"{test}.xml"
        if not xml_path.is_file():
            log(f"WARNING: {xml_path} not found — skipping test {test}")
            continue
        test_files[test] = f"{test}.xml"

    # Set up working directory
    work_dir = args.work_dir or (repo_root / "tmp/double_check_perf")
    work_dir.mkdir(parents=True, exist_ok=True)

    # Download binaries
    left_bin, right_bin = download_binaries(
        repo_root, work_dir, pr_number, pr_sha, ref_sha, build_type
    )
    log(f"left  binary: {left_bin}")
    log(f"right binary: {right_bin}")

    # Prepare top-level domains
    tld_src = repo_root / "tests/config/top_level_domains"
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
        prepare_configs(repo_root, side_dir)

    prepare_dataset(db_source, left_bin, work_dir / "left" / "config",
                    tld_dst, work_dir)

    for side in ("left", "right"):
        side_dir = work_dir / side
        hardlink_db(db_source, side_dir / "db")

    # Start servers
    left_h = start_server(
        work_dir / "left", "left",
        LEFT_TCP, LEFT_KEEPER_TCP, LEFT_KEEPER_RAFT, LEFT_INTERSERVER, tld_dst,
    )
    right_h = start_server(
        work_dir / "right", "right",
        RIGHT_TCP, RIGHT_KEEPER_TCP, RIGHT_KEEPER_RAFT, RIGHT_INTERSERVER, tld_dst,
    )

    try:
        if not wait_server_ready(left_h):
            die(f"left server failed to start; see {left_h.log_path}")
        if not wait_server_ready(right_h):
            die(f"right server failed to start; see {right_h.log_path}")
        log("both servers ready")
        # If the dataset directory was just freshly populated, ClickHouse
        # will be busy consolidating parts that came in at various merge
        # levels. Measuring before those settle yields unstable numbers.
        if not args.skip_wait_for_merges:
            wait_for_merges([left_h, right_h])

        # Run perf.py per test
        for test, qs in sorted(by_test.items()):
            if test not in test_files:
                continue
            run_perf_test(repo_root, work_dir, test_files[test], sorted(qs), args.runs)

        # Collect results
        local_results: dict[tuple[str, int], dict] = {}
        for test in by_test:
            raw = work_dir / "raw" / f"{test}-raw.tsv"
            diffs = parse_perf_diffs(raw)
            for qi, d in diffs.items():
                local_results[(test, qi)] = d

        print_report(changed, local_results, perf_arch)
        # JSON dump for downstream use
        json_path = work_dir / "result.json"
        json_path.write_text(
            json.dumps(
                {
                    "commit": pr_sha,
                    "pr": pr_number,
                    "arch": perf_arch,
                    "reference_sha": ref_sha,
                    "changed": [cq.__dict__ for cq in changed],
                    "local": {
                        f"{k[0]}#{k[1]}": v for k, v in local_results.items()
                    },
                },
                indent=2,
            )
        )
        log(f"wrote {json_path}")
    finally:
        stop_server(right_h)
        stop_server(left_h)

    return 0


if __name__ == "__main__":
    sys.exit(main())
