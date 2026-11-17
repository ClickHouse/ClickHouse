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
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
from urllib.request import urlopen
from urllib.error import HTTPError


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def fetch_url(url):
    """Fetch a URL and return its body as text."""
    try:
        with urlopen(url, timeout=60) as resp:
            return resp.read().decode("utf-8")
    except HTTPError as e:
        raise RuntimeError(f"HTTP {e.code}: {e.reason} for {url}")


def download_url(url, dest):
    """Download a URL to a file using curl. Returns True on success."""
    result = subprocess.run(
        ["curl", "-sfL", "-o", dest, url],
        capture_output=True, timeout=120,
    )
    return result.returncode == 0


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

    if not download_url(shard["tsv_url"], dest):
        return shard, None, f"Failed to download {shard['tsv_url']}"

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

def _sql_escape(s):
    """Escape a string for use in SQL single-quoted literals."""
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _build_base_cte(args, data_path):
    """Build the common CTE that reads, filters, and classifies data."""
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
            toFloat64(c8) AS stat_threshold,
            c9 AS test,
            toUInt32(c10) AS query_index,
            c11 AS query_display_name
        FROM file('{data_path}', 'TSV')
    ),
    filtered AS (
        SELECT *,
            (abs(diff) >= stat_threshold AND abs(diff) > 0.1) AS is_changed,
            (NOT (abs(diff) >= stat_threshold AND abs(diff) > 0.1) AND stat_threshold > 0.2) AS is_unstable,
            if(diff > 0, 'slower', if(diff < 0, 'faster', 'same')) AS direction
        FROM data
        WHERE {where_clause}
    )"""


def build_summary_sql(args, shard_meta, data_path):
    """Build SQL for per-shard summary."""
    base_cte = _build_base_cte(args, data_path)
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


def build_detail_sql(args, data_path, fmt="JSONEachRow"):
    """Build SQL for per-query detail rows."""
    base_cte = _build_base_cte(args, data_path)

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


def output_tsv(args, data_path):
    """Run the detail query with TabSeparatedWithNames format and print."""
    sql = build_detail_sql(args, data_path, fmt="TabSeparatedWithNames")
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

    # Verify clickhouse is available
    if shutil.which("clickhouse") is None:
        print("Error: clickhouse not found in PATH.", file=sys.stderr)
        sys.exit(1)

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

        # Run summary query
        summary_sql = build_summary_sql(args, shard_meta, merged_path)
        summary_rows = parse_jsonl(run_ch(summary_sql))

        if args.tsv:
            output_tsv(args, merged_path)
        elif args.json:
            detail_sql = build_detail_sql(args, merged_path)
            detail_rows = parse_jsonl(run_ch(detail_sql))
            output_json(summary_rows, detail_rows, pr_number, sha, args.metric)
        elif args.summary:
            output_human(summary_rows, [], pr_number, args.metric, multi_shard)
        else:
            detail_sql = build_detail_sql(args, merged_path)
            detail_rows = parse_jsonl(run_ch(detail_sql))
            output_human(summary_rows, detail_rows, pr_number, args.metric, multi_shard)

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
