#!/usr/bin/env python3
"""
Fetch ClickHouse CI performance comparison results.

Fetches the machine-readable all-query-metrics.tsv from S3 for each
performance comparison shard, then uses clickhouse-local to filter,
classify, and format the results.

Usage:
  python3 fetch_perf_report.py <url> [options]

URL formats:
  - GitHub PR URL:  https://github.com/ClickHouse/ClickHouse/pull/12345
  - CI HTML URL:    https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=...&sha=...

Options:
  --arch <amd|arm|all>   Filter by architecture (default: all)
  --metric <name>        Filter by metric name (default: client_time)
  --all                  Show all queries, not just significant changes
  --shard <n>            Show only shard n (1-based)
  --test <name>          Filter by test name substring
  --query <text>         Filter by query text substring
  --sort <field>         Sort by: diff, times, threshold, test (default: diff)
  --json                 Output as JSON
  --tsv                  Output raw TSV (for piping)
  --summary              Show only per-shard summary, no individual queries

Examples:
  python3 fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/96630"
  python3 fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/96630" --arch amd
  python3 fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/96630" --all --sort times
  python3 fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/96630" --test group_by
  python3 fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/96630" --json
"""

import argparse
import gzip
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
from urllib.request import urlopen
from urllib.error import HTTPError


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def maybe_decompress(data):
    """Transparently decompress `data` (bytes) if it is zstd- or gzip-framed.

    CI uploads text artifacts larger than a threshold as zstd (see
    ci/praktika/s3.py), so the same object can arrive either plain or compressed.
    Detected by magic bytes, so it works regardless of the URL suffix.
    """
    if data[:4] == b"\x28\xb5\x2f\xfd":  # zstd magic
        try:
            import zstandard  # optional dependency
            return zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)).read()
        except ImportError:
            proc = subprocess.run(["zstd", "-dcq"], input=data, capture_output=True, timeout=120)
            if proc.returncode != 0:
                raise RuntimeError(f"zstd decompression failed: {proc.stderr.decode('utf-8', 'replace')}")
            return proc.stdout
    if data[:2] == b"\x1f\x8b":  # gzip magic
        return gzip.decompress(data)
    return data


def _read_url_bytes(url):
    """GET `url`, falling back to `url + '.zst'` on 404/403, and return decompressed bytes."""
    candidates = [url] if url.endswith(".zst") else [url, url + ".zst"]
    last_error = None
    for candidate in candidates:
        try:
            with urlopen(candidate, timeout=60) as resp:
                return maybe_decompress(resp.read())
        except HTTPError as e:
            last_error = f"HTTP {e.code}: {e.reason} for {candidate}"
    raise RuntimeError(last_error)


def fetch_url(url):
    """Fetch a URL and return its body as text (transparently decompressed)."""
    return _read_url_bytes(url).decode("utf-8")


class _PrefixedReader:
    """File-like wrapper that yields `prefix` bytes before the rest of `stream`.

    Lets us peek at the magic bytes for format detection and still stream the whole
    body through a decompressor without ever holding it fully in memory.
    """

    def __init__(self, prefix, stream):
        self._prefix = prefix
        self._stream = stream

    def read(self, size=-1):
        if not self._prefix:
            return self._stream.read(size)
        if size is None or size < 0:
            data, self._prefix = self._prefix + self._stream.read(), b""
            return data
        if size <= len(self._prefix):
            data, self._prefix = self._prefix[:size], self._prefix[size:]
            return data
        data, self._prefix = self._prefix + self._stream.read(size - len(self._prefix)), b""
        return data


_ZSTD_CLI_TIMEOUT_SEC = 120  # watchdog bound for the zstd-CLI decompression fallback


def _stream_to_file(resp, dest):
    """Stream `resp` to `dest`, transparently decompressing zstd/gzip by magic bytes.

    Compressed perf artifacts exist precisely for the large shards, and several shards
    download in parallel, so the body is streamed in chunks rather than buffered.
    """
    header = resp.read(4)
    source = _PrefixedReader(header, resp)
    with open(dest, "wb") as f:
        if header == b"\x28\xb5\x2f\xfd":  # zstd magic
            try:
                import zstandard  # optional dependency
                shutil.copyfileobj(zstandard.ZstdDecompressor().stream_reader(source), f)
            except ImportError:
                proc = subprocess.Popen(["zstd", "-dcq"], stdin=subprocess.PIPE, stdout=f,
                                        stderr=subprocess.PIPE)
                # A watchdog bounds the whole exchange: both the stdin write and the stderr read
                # block indefinitely if the child wedges, so wait(timeout=) alone never fires.
                # Killing the child unblocks both and turns a stuck shard into a normal error.
                # (communicate() is unusable here: it flushes the already-closed stdin, which
                # raises ValueError on most Python versions.)
                watchdog = threading.Timer(_ZSTD_CLI_TIMEOUT_SEC, proc.kill)
                watchdog.start()
                try:
                    try:
                        shutil.copyfileobj(source, proc.stdin)
                        proc.stdin.close()
                    except BrokenPipeError:
                        pass  # zstd exited early (corrupt input or killed); real error is on stderr
                    stderr = proc.stderr.read()
                    proc.stderr.close()
                    rc = proc.wait()
                finally:
                    watchdog.cancel()
                if rc != 0:
                    raise RuntimeError(f"zstd decompression failed (exit {rc}): "
                                       f"{stderr.decode('utf-8', 'replace')}")
        elif header[:2] == b"\x1f\x8b":  # gzip magic
            with gzip.GzipFile(fileobj=source) as gz:
                shutil.copyfileobj(gz, f)
        else:
            shutil.copyfileobj(source, f)


def download_url(url, dest):
    """Download a URL to a file, transparently decompressing. Raises on failure.

    Falls back to `url + '.zst'` on 404/403. Shard-local failures are isolated by the
    `download_shard` worker, not here.
    """
    candidates = [url] if url.endswith(".zst") else [url, url + ".zst"]
    last_error = None
    for candidate in candidates:
        try:
            with urlopen(candidate, timeout=60) as resp:
                _stream_to_file(resp, dest)
                return
        except HTTPError as e:
            last_error = f"HTTP {e.code}: {e.reason} for {candidate}"
    raise RuntimeError(last_error)


# ---------------------------------------------------------------------------
# PR / report resolution
# ---------------------------------------------------------------------------

def resolve_pr(pr_url):
    """Given a GitHub PR URL, resolve base_url, pr_number, sha."""
    m = re.search(
        r"github\.com/ClickHouse/(ClickHouse(?:_private)?|clickhouse-private)/pull/(\d+)",
        pr_url,
    )
    if not m:
        raise RuntimeError("Invalid GitHub PR URL")
    repo = m.group(1)
    pr_number = m.group(2)

    is_private = "private" in repo.lower()
    bucket = "clickhouse-private-test-reports" if is_private else "clickhouse-test-reports"
    base_url = f"https://s3.amazonaws.com/{bucket}"

    gh_repo = f"ClickHouse/{repo}" if is_private else "ClickHouse/ClickHouse"
    try:
        comments_json = subprocess.check_output(
            [
                "gh", "api", f"repos/{gh_repo}/issues/{pr_number}/comments",
                "--paginate",
                "--jq",
                '.[] | select(.user.login == "clickhouse-gh[bot]") | {body, created_at}',
            ],
            text=True,
            stderr=subprocess.PIPE,
        )
        comments = [json.loads(line) for line in comments_json.strip().splitlines() if line.strip()]
        comments.sort(key=lambda c: c.get("created_at", ""), reverse=True)
        if not comments:
            raise RuntimeError("No CI bot comment found")

        url_pattern = re.compile(
            r"https://s3\.amazonaws\.com/clickhouse(?:-private)?-test-reports/json\.html\?[^\s)]+"
        )
        ci_url = None
        for comment in comments:
            body = comment.get("body", "")
            urls = url_pattern.findall(body)
            if urls:
                ci_url = urls[0]
                break
        if not ci_url:
            raise RuntimeError("No CI report URLs found in bot comments")
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Failed to get CI info for PR #{pr_number}: {e}")

    parsed = urlparse(ci_url)
    params = parse_qs(parsed.query)
    sha = params.get("sha", [None])[0]
    if not sha:
        raise RuntimeError("No sha parameter in CI URL")

    if sha == "latest":
        commits_url = f"{base_url}/PRs/{pr_number}/commits.json"
        commits = json.loads(fetch_url(commits_url))
        if not commits:
            raise RuntimeError("No commits found in commits.json")
        sha = commits[-1]["sha"]

    return {"base_url": base_url, "pr_number": pr_number, "sha": sha}


def resolve_html_url(html_url):
    """Given a CI HTML URL, extract base_url, pr_number, sha."""
    parsed = urlparse(html_url)
    params = parse_qs(parsed.query)

    pr = params.get("PR", [None])[0]
    sha = params.get("sha", [None])[0]
    base_url_param = params.get("base_url", [None])[0]

    if base_url_param:
        base_url = base_url_param
    else:
        path_parts = parsed.path.rsplit("/", 1)[0]
        base_url = f"{parsed.scheme}://{parsed.netloc}{path_parts}"

    if not pr:
        raise RuntimeError("PR parameter is required in URL")
    if not sha:
        raise RuntimeError("sha parameter is required in URL")

    if sha == "latest":
        commits_url = f"{base_url}/PRs/{pr}/commits.json"
        commits = json.loads(fetch_url(commits_url))
        if not commits:
            raise RuntimeError("No commits found")
        sha = commits[-1]["sha"]

    return {"base_url": base_url, "pr_number": pr, "sha": sha}


# ---------------------------------------------------------------------------
# Shard discovery
# ---------------------------------------------------------------------------

def normalize_job_name(name):
    """Normalize a job name to the S3 directory format."""
    result = name.lower()
    result = re.sub(r"[^a-z0-9]", "_", result)
    result = re.sub(r"_+", "_", result)
    result = result.rstrip("_")
    return result


def get_performance_shards(base_url, pr_number, sha):
    """Fetch the PR-level result JSON and extract perf shard info."""
    pr_json_url = f"{base_url}/PRs/{pr_number}/{sha}/result_pr.json"
    pr_json = json.loads(fetch_url(pr_json_url))

    shards = []

    def walk(results):
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

                    tsv_link = None
                    for link in r.get("links", []):
                        if isinstance(link, str) and "all-query-metrics.tsv" in link:
                            tsv_link = link
                            break

                    if not tsv_link:
                        dir_name = normalize_job_name(name)
                        tsv_link = f"{base_url}/PRs/{pr_number}/{sha}/{dir_name}/all-query-metrics.tsv"

                    shards.append({
                        "name": name,
                        "arch": arch,
                        "baseline": baseline,
                        "shard_num": shard_num,
                        "total_shards": total_shards,
                        "status": r.get("status", "unknown"),
                        "info": r.get("info", ""),
                        "tsv_url": tsv_link,
                    })
            if r.get("results"):
                walk(r["results"])

    walk(pr_json.get("results"))
    return shards


# ---------------------------------------------------------------------------
# Download shard TSV files
# ---------------------------------------------------------------------------

def download_shard(shard, tmpdir):
    """Download one shard's TSV, prepending arch/shard_num columns.
    Returns (shard, dest_path, error)."""
    arch = shard["arch"]
    shard_num = shard["shard_num"]
    dest = os.path.join(tmpdir, f"{arch}_{shard_num}.tsv")

    # Isolate all shard-local download/decompress failures (HTTP, network, timeout, missing zstd,
    # corrupt archive, ...) as a per-shard error so one bad shard warns and the report continues
    # with the rest rather than aborting the whole run.
    try:
        download_url(shard["tsv_url"], dest)
    except Exception as e:
        return shard, None, f"Failed to download {shard['tsv_url']}: {e}"

    # Prepend arch and shard_num columns so clickhouse-local can distinguish shards
    enriched = os.path.join(tmpdir, f"{arch}_{shard_num}_enriched.tsv")
    try:
        with open(dest, "r") as fin, open(enriched, "w") as fout:
            for line in fin:
                line = line.rstrip("\n")
                if line:
                    fout.write(f"{arch}\t{shard_num}\t{line}\n")
        os.unlink(dest)
        return shard, enriched, None
    except Exception as e:
        return shard, None, str(e)


# ---------------------------------------------------------------------------
# SQL queries
# ---------------------------------------------------------------------------

# Number of (enriched) columns in all-query-metrics.tsv once it carries the
# per-query changed_threshold/unstable_threshold columns. The two arch/shard
# columns are prepended by download_shard, the report itself contributes the
# rest, ending with changed_threshold (c12) and unstable_threshold (c13).
COLUMNS_WITH_THRESHOLDS = 13


def count_tsv_columns(path):
    """Return the number of tab-separated columns in the first non-empty line."""
    with open(path, "r") as f:
        for line in f:
            line = line.rstrip("\n")
            if line:
                return line.count("\t") + 1
    return 0


def _sql_escape(s):
    """Escape a string for use in SQL single-quoted literals."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _build_base_cte(args, data_path, has_thresholds):
    """Build the common CTE that reads, filters, and classifies data.

    When ``has_thresholds`` is true the report carries the per-query
    changed_threshold/unstable_threshold columns (c12/c13) exported by
    ci/jobs/scripts/perf/compare.sh, and we classify with them so this helper
    matches the CI gate exactly. Older reports do not have those columns, so we
    fall back to the floor constants (0.15/0.25); see count_tsv_columns and the
    warning emitted in main().
    """
    where_parts = [f"metric_name = '{_sql_escape(args.metric)}'"]
    if args.arch != "all":
        where_parts.append(f"arch = '{args.arch}'")
    if args.shard is not None:
        where_parts.append(f"shard_num = {args.shard}")
    if args.test:
        where_parts.append(f"positionCaseInsensitive(test, '{_sql_escape(args.test)}') > 0")
    if args.query:
        where_parts.append(f"positionCaseInsensitive(query_display_name, '{_sql_escape(args.query)}') > 0")

    where_clause = " AND ".join(where_parts)

    if has_thresholds:
        threshold_cols = """
            toFloat64(c12) AS changed_threshold,
            toFloat64(c13) AS unstable_threshold,"""
    else:
        threshold_cols = """
            0.15 AS changed_threshold,
            0.25 AS unstable_threshold,"""

    return f"""
    data AS (
        SELECT
            c1 AS arch,
            toUInt32(c2) AS shard_num,
            c3 AS metric_name,
            toFloat64(c4) AS `left`,
            toFloat64(c5) AS `right`,
            toFloat64(c6) AS diff,
            toFloat64(c7) AS times_change,
            toFloat64(c8) AS stat_threshold,{threshold_cols}
            c9 AS test,
            toUInt32(c10) AS query_index,
            c11 AS query_display_name
        FROM file('{data_path}', 'TSV')
    ),
    filtered AS (
        -- Classify each query exactly as ci/jobs/scripts/perf/compare.sh does:
        --   changed_fail  = abs(diff) > changed_threshold  AND abs(diff) >= stat_threshold
        --   unstable_fail = NOT changed_fail               AND stat_threshold > unstable_threshold
        -- changed_threshold/unstable_threshold are the per-query thresholds
        -- (the 0.15/0.25 floors raised by historical and per-test thresholds).
        -- Using the exported per-query thresholds instead of only the floor
        -- constants keeps this helper in agreement with the CI gate even when
        -- the effective threshold for a query is above the floor.
        SELECT *,
            (abs(diff) >= stat_threshold AND abs(diff) > changed_threshold) AS is_changed,
            (NOT (abs(diff) >= stat_threshold AND abs(diff) > changed_threshold) AND stat_threshold > unstable_threshold) AS is_unstable,
            if(diff > 0, 'slower', if(diff < 0, 'faster', 'same')) AS direction
        FROM data
        WHERE {where_clause}
    )"""


def build_summary_sql(args, shard_meta, data_path, has_thresholds):
    """Build SQL for per-shard summary."""
    base_cte = _build_base_cte(args, data_path, has_thresholds)
    shard_values = ", ".join(
        f"('{_sql_escape(s['name'])}', '{s['arch']}', {s['shard_num']})"
        for s in shard_meta
    )

    return f"""
    WITH {base_cte},
    shard_meta AS (
        SELECT * FROM VALUES(
            'name String, arch String, shard_num UInt32',
            {shard_values}
        )
    ),
    shard_stats AS (
        SELECT
            arch, shard_num,
            countIf(is_changed AND direction = 'faster') AS faster,
            countIf(is_changed AND direction = 'slower') AS slower,
            countIf(is_unstable) AS unstable,
            count() AS total
        FROM filtered
        GROUP BY arch, shard_num
    )
    SELECT
        m.name,
        coalesce(s.faster, 0) AS faster,
        coalesce(s.slower, 0) AS slower,
        coalesce(s.unstable, 0) AS unstable,
        coalesce(s.total, 0) AS total
    FROM shard_meta m
    LEFT JOIN shard_stats s ON m.arch = s.arch AND m.shard_num = s.shard_num
    ORDER BY m.arch, m.shard_num
    FORMAT JSONEachRow
    """


def build_detail_sql(args, data_path, has_thresholds, fmt="JSONEachRow"):
    """Build SQL for per-query detail rows."""
    base_cte = _build_base_cte(args, data_path, has_thresholds)

    sort_map = {
        "diff": "abs(diff) DESC",
        "times": "times_change DESC",
        "threshold": "stat_threshold DESC",
        "test": "test ASC, query_index ASC",
    }
    sort_clause = sort_map.get(args.sort, "abs(diff) DESC")

    display_filter = "" if args.show_all else "WHERE is_changed OR is_unstable"

    return f"""
    WITH {base_cte}
    SELECT
        test,
        query_index,
        arch,
        shard_num AS shard,
        `left` AS old,
        `right` AS new,
        diff,
        times_change,
        stat_threshold,
        is_changed,
        is_unstable,
        direction,
        query_display_name AS query
    FROM filtered
    {display_filter}
    ORDER BY {sort_clause}
    FORMAT {fmt}
    """


# ---------------------------------------------------------------------------
# Run clickhouse-local
# ---------------------------------------------------------------------------

def run_ch(sql):
    """Run a SQL query via clickhouse local. Returns stdout."""
    cmd = ["clickhouse", "local", "--query", sql]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        raise RuntimeError(
            f"clickhouse local failed (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    return result.stdout


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def fmt_diff(diff):
    sign = "+" if diff > 0 else ""
    return f"{sign}{diff * 100:.1f}%"


def fmt_times(left, right):
    if left == 0 and right == 0:
        return "--"
    if right == 0:
        return "+inf"
    if left == 0:
        return "-inf"
    ratio = left / right if left > right else right / left
    sign = "-" if left > right else "+"
    return f"{sign}{ratio:.3f}x"


def fmt_seconds(v):
    if v >= 1:
        return f"{v:.3f}s"
    if v >= 0.001:
        return f"{v * 1000:.1f}ms"
    return f"{v * 1e6:.0f}us"


def truncate(s, max_len=80):
    return s[:max_len - 3] + "..." if len(s) > max_len else s


def parse_jsonl(text):
    """Parse newline-delimited JSON."""
    rows = []
    for line in text.strip().split("\n"):
        if line:
            rows.append(json.loads(line))
    return rows


def output_json(summary_rows, detail_rows, pr_number, sha, metric):
    """Assemble and print the JSON output."""
    output = {
        "pr": pr_number,
        "sha": sha,
        "metric": metric,
        "shards": [
            {
                "name": s["name"],
                "faster": s["faster"],
                "slower": s["slower"],
                "unstable": s["unstable"],
                "total": s["total"],
            }
            for s in summary_rows
        ],
        "queries": detail_rows,
    }
    print(json.dumps(output, indent=2))


def output_tsv(args, data_path, has_thresholds):
    """Run the detail query with TabSeparatedWithNames format and print."""
    sql = build_detail_sql(args, data_path, has_thresholds, fmt="TabSeparatedWithNames")
    print(run_ch(sql), end="")


def output_human(summary_rows, detail_rows, pr_number, metric, multi_shard):
    """Print human-readable output."""
    print("=" * 90)
    print(f"PERFORMANCE COMPARISON  PR #{pr_number}  (metric: {metric})")
    print("=" * 90)
    print()

    total_faster = total_slower = total_unstable = total_queries = 0

    for s in summary_rows:
        faster, slower, unstable, total = s["faster"], s["slower"], s["unstable"], s["total"]

        status_icon = "WARN" if slower > 0 else "OK"
        info_parts = []
        if faster > 0:
            info_parts.append(f"{faster} faster")
        if slower > 0:
            info_parts.append(f"{slower} slower")
        if unstable > 0:
            info_parts.append(f"{unstable} unstable")
        if not info_parts:
            info_parts.append("no changes")

        print(f"[{status_icon:>4}] {s['name']}  -- {', '.join(info_parts)} ({total} queries)")

        total_faster += faster
        total_slower += slower
        total_unstable += unstable
        total_queries += total

    print()
    print(
        f"Total: {total_queries} queries | "
        f"{total_faster} faster | {total_slower} slower | "
        f"{total_unstable} unstable"
    )
    print()

    if not detail_rows:
        print("No significant performance changes detected.")
        return

    changed = [r for r in detail_rows if r["is_changed"]]
    unstable = [r for r in detail_rows if r["is_unstable"] and not r["is_changed"]]
    unchanged = [r for r in detail_rows if not r["is_changed"] and not r["is_unstable"]]

    if changed:
        print("-" * 90)
        print("CHANGES IN PERFORMANCE")
        print("-" * 90)
        print()

        slower_qs = [q for q in changed if q["direction"] == "slower"]
        faster_qs = [q for q in changed if q["direction"] == "faster"]

        for label, qs in [("SLOWER", slower_qs), ("FASTER", faster_qs)]:
            if not qs:
                continue
            print(f"  {label} ({len(qs)}):")
            print()
            for q in qs:
                ratio = fmt_times(q["old"], q["new"])
                diff = fmt_diff(q["diff"])
                arch_tag = f" [{q['arch']}/{q['shard']}]" if multi_shard else ""
                print(
                    f"    {ratio:>10}  {diff:>8}  "
                    f"{fmt_seconds(q['old']):>8} -> {fmt_seconds(q['new']):>8}  "
                    f"{q['test']} #{q['query_index']}{arch_tag}"
                )
                print(
                    f"{'':>16}  threshold: {fmt_diff(q['stat_threshold'])}  "
                    f"query: {truncate(q['query'])}"
                )
            print()

    if unstable:
        print("-" * 90)
        print(f"UNSTABLE QUERIES ({len(unstable)})")
        print("-" * 90)
        print()
        for q in unstable:
            diff = fmt_diff(q["diff"])
            arch_tag = f" [{q['arch']}/{q['shard']}]" if multi_shard else ""
            print(
                f"    {diff:>8}  threshold: {fmt_diff(q['stat_threshold'])}  "
                f"{q['test']} #{q['query_index']}{arch_tag}"
            )
            print(
                f"{'':>12}  {fmt_seconds(q['old']):>8} -> {fmt_seconds(q['new']):>8}  "
                f"query: {truncate(q['query'])}"
            )
        print()

    if unchanged:
        print("-" * 90)
        print(f"ALL QUERIES ({len(unchanged)} unchanged)")
        print("-" * 90)
        print()
        for q in unchanged:
            diff = fmt_diff(q["diff"])
            arch_tag = f" [{q['arch']}/{q['shard']}]" if multi_shard else ""
            print(
                f"    {diff:>8}  {fmt_seconds(q['old']):>8} -> {fmt_seconds(q['new']):>8}  "
                f"{q['test']} #{q['query_index']}{arch_tag}"
            )
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch ClickHouse CI performance comparison results"
    )
    parser.add_argument("url", help="GitHub PR URL or CI HTML URL")
    parser.add_argument(
        "--arch", default="all", choices=["amd", "arm", "all"],
        help="Filter by architecture (default: all)",
    )
    parser.add_argument(
        "--metric", default="client_time",
        help="Metric to analyze (default: client_time)",
    )
    parser.add_argument(
        "--all", action="store_true", dest="show_all",
        help="Show all queries, not just changes/unstable",
    )
    parser.add_argument(
        "--shard", type=int, default=None,
        help="Show only shard n (1-based)",
    )
    parser.add_argument(
        "--test", default=None,
        help="Filter by test name substring",
    )
    parser.add_argument(
        "--query", default=None,
        help="Filter by query text substring",
    )
    parser.add_argument(
        "--sort", default="diff", choices=["diff", "times", "threshold", "test"],
        help="Sort by field (default: diff)",
    )
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--tsv", action="store_true", help="Output raw TSV (for piping)")
    parser.add_argument(
        "--summary", action="store_true",
        help="Only show per-shard summary",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Resolve URL
    url = args.url
    if "github.com" in url and "/pull/" in url:
        resolved = resolve_pr(url)
    elif "json.html" in url:
        resolved = resolve_html_url(url)
    else:
        print("Error: URL must be a GitHub PR URL or CI HTML URL", file=sys.stderr)
        sys.exit(1)

    base_url = resolved["base_url"]
    pr_number = resolved["pr_number"]
    sha = resolved["sha"]
    print(f"PR #{pr_number}, SHA: {sha[:12]}\n", file=sys.stderr)

    # Get performance shards
    shards = get_performance_shards(base_url, pr_number, sha)
    if not shards:
        print("No performance comparison shards found", file=sys.stderr)
        sys.exit(1)

    # Filter before download
    if args.arch != "all":
        shards = [s for s in shards if s["arch"] == args.arch]
    if args.shard is not None:
        shards = [s for s in shards if s["shard_num"] == args.shard]

    if not shards:
        print("No matching shards found", file=sys.stderr)
        sys.exit(1)

    # Jobs that didn't run (e.g. the amd perf comparison only runs on 'pr-performance' PRs) have no
    # artifacts, so report them cleanly instead of attempting a download that 403s.
    skipped = [s for s in shards if str(s.get("status", "")).upper() == "SKIPPED"]
    for s in skipped:
        print(f"  Skipped: {s['name']} ({s.get('info') or 'not run'})", file=sys.stderr)
    shards = [s for s in shards if str(s.get("status", "")).upper() != "SKIPPED"]

    if not shards:
        # All matching jobs were intentionally skipped: no perf data to analyze, but not a
        # tool failure, so exit successfully.
        print("No shards to fetch (all matching jobs were skipped)", file=sys.stderr)
        sys.exit(0)

    # clickhouse (used for local classification of the downloaded data) is only needed once we
    # know there is at least one shard to analyze.
    if shutil.which("clickhouse") is None:
        print("Error: clickhouse not found in PATH.", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching {len(shards)} performance shard(s)...\n", file=sys.stderr)

    # Download TSV data in parallel
    tmpdir = tempfile.mkdtemp(prefix="perf_report_")
    downloaded = []

    try:
        with ThreadPoolExecutor(max_workers=min(len(shards), 8)) as pool:
            futures = {
                pool.submit(download_shard, shard, tmpdir): shard
                for shard in shards
            }
            for future in as_completed(futures):
                shard, path, error = future.result()
                if error:
                    print(f"  Warning: {shard['name']}: {error}", file=sys.stderr)
                else:
                    downloaded.append((shard, path))

        if not downloaded:
            print("Error: Failed to download any shard data", file=sys.stderr)
            sys.exit(1)

        downloaded.sort(key=lambda x: (x[0]["arch"], x[0]["shard_num"]))

        # Merge all enriched TSV files into one
        merged_path = os.path.join(tmpdir, "all.tsv")
        with open(merged_path, "w") as fout:
            for _, path in downloaded:
                with open(path, "r") as fin:
                    fout.write(fin.read())

        shard_meta = [
            {"name": s["name"], "arch": s["arch"], "shard_num": s["shard_num"]}
            for s, _ in downloaded
        ]
        multi_shard = len(downloaded) > 1

        # Detect whether the report carries the per-query threshold columns.
        # Reports generated before those columns were added to
        # all-query-metrics.tsv lack them, so we classify with the floor
        # constants instead and make that visible rather than silently
        # reporting queries that CI would treat as noise.
        has_thresholds = count_tsv_columns(merged_path) >= COLUMNS_WITH_THRESHOLDS
        if not has_thresholds:
            print(
                "  Warning: this report predates the per-query threshold columns "
                "in all-query-metrics.tsv; classifying with the 0.15/0.25 floor "
                "constants, which may flag queries that CI treats as noise.",
                file=sys.stderr,
            )

        # Run summary query
        summary_sql = build_summary_sql(args, shard_meta, merged_path, has_thresholds)
        summary_rows = parse_jsonl(run_ch(summary_sql))

        if args.tsv:
            output_tsv(args, merged_path, has_thresholds)
        elif args.json:
            detail_sql = build_detail_sql(args, merged_path, has_thresholds)
            detail_rows = parse_jsonl(run_ch(detail_sql))
            output_json(summary_rows, detail_rows, pr_number, sha, args.metric)
        elif args.summary:
            output_human(summary_rows, [], pr_number, args.metric, multi_shard)
        else:
            detail_sql = build_detail_sql(args, merged_path, has_thresholds)
            detail_rows = parse_jsonl(run_ch(detail_sql))
            output_human(summary_rows, detail_rows, pr_number, args.metric, multi_shard)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
