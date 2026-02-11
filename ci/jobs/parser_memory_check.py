#!/usr/bin/env python3
"""
Parser AST Memory Profiler CI Check

Compares parser AST memory allocations between the PR build and master build.
Uses jemalloc heap profiles (not stats.allocated) as the source of truth.
Results appear directly in the praktika CI report (json.html) as test cases.
A standalone HTML report is also generated and attached to the CI result.
"""

import glob
import os
import re
import subprocess
from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

TEMP_DIR = f"{Utils.cwd()}/ci/tmp"
QUERIES_FILE = f"{Utils.cwd()}/utils/parser-memory-profiler/test_queries.txt"
MASTER_PROFILER_URL = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/parser_memory_profiler"

# Threshold: changes below this are considered OK (noise)
CHANGE_THRESHOLD_BYTES = 32

# Frame prefixes to strip from stack tops (jemalloc/malloc internals, profiler overhead)
NOISE_FRAME_PREFIXES = (
    "je_",
    "imalloc",
    "iallocztm",
    "ialloc",
    "ifree",
    "arena_",
    "tcache_",
    "prof_",
    "malloc",
    "calloc",
    "realloc",
    "free",
    "operator new",
    "operator delete",
    "__libc_",
    "__GI_",
    "_int_malloc",
    "_int_free",
    "isalloc",
    "sdallocx",
    "rallocx",
    "mallocx",
    "main",
    "(anonymous namespace)::dumpProfile",
    "dumpProfile",
    "kdf_sshkdf_free",
)


def download_master_binary(dest_path: str) -> bool:
    """Download master parser_memory_profiler from S3."""
    print(f"Downloading master binary from {MASTER_PROFILER_URL}")
    return Shell.check(
        f"wget -nv -O {dest_path} {MASTER_PROFILER_URL} && chmod +x {dest_path}"
    )


def parse_symbolized_heap(sym_file: str) -> dict:
    """
    Parse a symbolized .heap.sym file produced by symbolizeHeapProfile().

    Format:
        --- symbol
        binary=/path/to/binary
        0x1234 DB::parseQuery--DB::ParserSelectQuery::parseImpl
        ---
        --- heap
        heap_v2/1
          t*: <curobjs>: <curbytes> [<accumobjs>: <accumbytes>]
        @ 0x1234 0x5678
          t*: <curobjs>: <curbytes> [<accumobjs>: <accumbytes>]

    Returns dict: { "stack_key" -> { "bytes": int, "stack": "symbolized;frames" } }
    """
    if not os.path.exists(sym_file):
        print(f"Warning: symbolized file not found: {sym_file}")
        return {}

    with open(sym_file, "r") as f:
        content = f.read()

    # Parse symbol table: int(address) -> "symbol1--symbol2"
    # Use int keys to normalize zero-padded (0x000000010726faef) vs
    # compact (0x10726faef) address representations.
    symbols = {}
    in_symbol_section = False
    in_heap_section = False
    lines = content.split("\n")

    for line in lines:
        if line.strip() == "--- symbol":
            in_symbol_section = True
            continue
        if line.strip() == "---" and in_symbol_section:
            in_symbol_section = False
            continue
        if line.strip() == "--- heap":
            in_heap_section = True
            continue

        if in_symbol_section:
            if line.startswith("binary="):
                continue
            # Format: 0x1234ABCD symbolname--inlinename
            parts = line.strip().split(" ", 1)
            if len(parts) == 2 and parts[0].startswith("0x"):
                try:
                    addr_int = int(parts[0], 16)
                    symbols[addr_int] = parts[1]
                except ValueError:
                    pass

    # Parse heap section: extract stacks with byte counts
    stacks = {}
    current_addrs = None
    current_addr_ints = None

    for line in lines:
        if not in_heap_section:
            if line.strip() == "--- heap":
                in_heap_section = True
            continue

        if line.startswith("@"):
            # Stack trace line: @ 0x1234 0x5678 ...
            addr_strs = line[1:].strip().split()
            current_addrs = []
            current_addr_ints = []
            for a in addr_strs:
                a_clean = a.lower()
                if not a_clean.startswith("0x"):
                    a_clean = "0x" + a_clean
                current_addrs.append(a_clean)
                try:
                    current_addr_ints.append(int(a_clean, 16))
                except ValueError:
                    current_addr_ints.append(0)

        elif current_addrs is not None and line.strip().startswith("t*:"):
            # Thread stats line: t*: curobjs: curbytes [accumobjs: accumbytes]
            match = re.match(r"\s*t\*:\s*(\d+):\s*(\d+)\s*\[", line)
            if match:
                curbytes = int(match.group(2))
                if curbytes > 0:
                    # Symbolize the stack using int-key lookup
                    sym_frames = []
                    for i, addr_int in enumerate(current_addr_ints):
                        sym = symbols.get(addr_int, "")
                        if not sym:
                            # Try addr-1 (caller address fix, same as jeprof)
                            sym = symbols.get(addr_int - 1, "")
                        sym_frames.append(sym if sym else current_addrs[i])

                    stack_key = ";".join(current_addrs)
                    stacks[stack_key] = {
                        "bytes": curbytes,
                        "frames": sym_frames,
                    }
            current_addrs = None
            current_addr_ints = None

    return stacks


def filter_stack_frames(frames: list) -> list:
    """Strip leading jemalloc/malloc/libc/profiler-overhead frames.
    Returns empty list if all frames are noise (profiler overhead)."""
    filtered = []
    found_clickhouse = False
    for frame in frames:
        if not found_clickhouse:
            # Check if this is a noise frame
            is_noise = False
            for prefix in NOISE_FRAME_PREFIXES:
                if frame.startswith(prefix):
                    is_noise = True
                    break
            if is_noise:
                continue
            found_clickhouse = True
        filtered.append(frame)
    return filtered


def format_stack(frames: list) -> str:
    """Format symbolized frames as a compact one-liner.
    Returns empty string if all frames are noise (profiler overhead)."""
    filtered = filter_stack_frames(frames)
    if not filtered:
        return ""
    # Split multi-frame symbols (separated by --) and flatten
    flat = []
    for f in filtered:
        parts = f.split("--")
        flat.extend(parts)
    # Take the most relevant frames (bottom-up, skip very deep ones)
    if len(flat) > 6:
        return " > ".join(flat[:6]) + " > ..."
    return " > ".join(flat)


def compute_diff(stacks_before: dict, stacks_after: dict) -> tuple:
    """
    Compute per-stack byte diffs between before and after heap profiles.
    Skips stacks that are entirely profiler overhead (e.g. dumpProfile, main-only).
    Returns (total_diff, list of (diff_bytes, formatted_stack) sorted by |diff| desc).
    """
    all_keys = set(stacks_before.keys()) | set(stacks_after.keys())
    diffs = []
    total = 0

    for key in all_keys:
        before_bytes = stacks_before.get(key, {}).get("bytes", 0)
        after_bytes = stacks_after.get(key, {}).get("bytes", 0)
        diff = after_bytes - before_bytes
        if diff != 0:
            frames = stacks_after.get(key, stacks_before.get(key, {})).get(
                "frames", [key]
            )
            formatted = format_stack(frames)
            if not formatted:
                # All frames are profiler overhead — skip
                continue
            diffs.append((diff, formatted))
            total += diff

    diffs.sort(key=lambda x: -abs(x[0]))
    return total, diffs


def html_escape(s: str) -> str:
    """Escape HTML special characters."""
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def format_profile_html(stacks, total_bytes, label):
    """Format a list of (bytes, stack) tuples into an HTML profile block."""
    if not stacks:
        return (
            f'<div class="profile-section">'
            f'<div class="profile-header">{html_escape(label)}: {total_bytes:,} bytes</div>'
            f'<div class="profile-empty">(no stack data)</div></div>'
        )

    rows = ""
    for diff_bytes, stack in stacks:
        pct = (diff_bytes / total_bytes * 100) if total_bytes else 0
        bar_width = min(abs(pct), 100)
        rows += (
            f'<div class="stack-row">'
            f'<span class="stack-bytes">{diff_bytes:+,} B</span>'
            f'<span class="stack-bar-wrap"><span class="stack-bar" style="width:{bar_width:.0f}%"></span></span>'
            f'<span class="stack-trace">{html_escape(stack)}</span></div>'
        )

    return (
        f'<div class="profile-section">'
        f'<div class="profile-header">{html_escape(label)}: {total_bytes:,} bytes ({len(stacks)} stacks)</div>'
        f"{rows}</div>"
    )


def generate_html_report(
    query_results, total_master, total_pr, total_regressions, total_improvements, duration, output_path
):
    """Generate a standalone HTML report viewable in browser."""
    total_change = total_pr - total_master
    overall_status = "FAIL" if total_regressions > 0 else "OK"
    num_queries = len(query_results)

    # Build table rows
    rows_html = ""
    for r in query_results:
        change = r["change"]
        status = r["status"]

        if status == "FAIL":
            row_class = "regression"
            status_badge = '<span class="badge badge-fail">FAIL</span>'
        elif status == "ERROR":
            row_class = "error"
            status_badge = '<span class="badge badge-error">ERROR</span>'
        elif change <= -CHANGE_THRESHOLD_BYTES:
            row_class = "improvement"
            status_badge = '<span class="badge badge-ok">OK</span>'
        else:
            row_class = ""
            status_badge = '<span class="badge badge-ok">OK</span>'

        change_str = f"{change:+,}" if change != 0 else "0"
        query_escaped = html_escape(r["query"])

        master_profile = format_profile_html(
            r.get("master_stacks", []), r["master_bytes"], "Master profile"
        )
        pr_profile = format_profile_html(
            r.get("pr_stacks", []), r["pr_bytes"], "PR profile"
        )

        detail_html = (
            f'<div class="detail-content">'
            f'<div class="detail-query"><strong>Query:</strong> <code>{query_escaped}</code></div>'
            f'<div class="detail-summary">'
            f'<span>Master: <strong>{r["master_bytes"]:,}</strong> B</span>'
            f'<span>PR: <strong>{r["pr_bytes"]:,}</strong> B</span>'
            f'<span>Change: <strong>{change_str}</strong> B</span>'
            f"</div>"
            f'<div class="profiles-container">{master_profile}{pr_profile}</div>'
            f"</div>"
        )

        rows_html += (
            f'<tr class="{row_class}" onclick="toggleDetail(this)">'
            f"<td>{r['num']}</td>"
            f'<td class="query-cell" title="{query_escaped}">{html_escape(r["query_display"])}</td>'
            f'<td class="num">{r["master_bytes"]:,}</td>'
            f'<td class="num">{r["pr_bytes"]:,}</td>'
            f'<td class="num">{change_str}</td>'
            f"<td>{status_badge}</td>"
            f"</tr>\n"
            f'<tr class="detail-row" style="display:none">'
            f"<td colspan=\"6\">{detail_html}</td>"
            f"</tr>\n"
        )

    # Use doubled braces for CSS/JS in f-string
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Parser AST Memory Check Report</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #fafafa; color: #333; padding: 24px; max-width: 100vw; overflow-x: hidden; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; font-size: 13px; margin-bottom: 20px; }}
  .summary {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; padding: 16px 20px; margin-bottom: 20px; display: flex; gap: 32px; flex-wrap: wrap; align-items: center; }}
  .summary-item {{ display: flex; flex-direction: column; }}
  .summary-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}
  .summary-value {{ font-size: 18px; font-weight: 600; }}
  .summary-value.ok {{ color: #388e3c; }}
  .summary-value.fail {{ color: #d32f2f; }}
  table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; overflow: hidden; table-layout: fixed; }}
  th {{ background: #f5f5f5; border-bottom: 2px solid #e0e0e0; padding: 10px 12px; text-align: left; font-size: 12px; color: #555; text-transform: uppercase; letter-spacing: 0.5px; cursor: pointer; user-select: none; white-space: nowrap; }}
  th:hover {{ background: #eee; }}
  th .sort-arrow {{ font-size: 10px; margin-left: 4px; color: #aaa; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #f0f0f0; font-size: 13px; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-family: 'SF Mono', 'Consolas', monospace; }}
  td.query-cell {{ font-family: 'SF Mono', 'Consolas', monospace; font-size: 12px; max-width: 420px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
  tr:hover:not(.detail-row) {{ background: #f8f9fa; cursor: pointer; }}
  tr.regression {{ background: #fff5f5; }}
  tr.regression:hover {{ background: #ffebee; }}
  tr.improvement {{ background: #f1f8e9; }}
  tr.improvement:hover {{ background: #e8f5e9; }}
  tr.error {{ background: #fff8e1; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }}
  .badge-ok {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-fail {{ background: #ffebee; color: #c62828; }}
  .badge-error {{ background: #fff8e1; color: #f57f17; }}
  .detail-row td {{ padding: 0; overflow: hidden; }}
  .detail-content {{ padding: 12px 16px; background: #fafafa; border-top: 1px dashed #ccc; max-width: 100%; overflow: hidden; }}
  .detail-query {{ margin-bottom: 8px; font-size: 13px; }}
  .detail-query code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px; font-size: 12px; word-break: break-all; }}
  .detail-summary {{ display: flex; gap: 24px; margin-bottom: 12px; font-size: 13px; color: #555; }}
  .detail-summary strong {{ color: #333; }}
  .profiles-container {{ display: flex; gap: 16px; width: 100%; overflow: hidden; }}
  .profile-section {{ flex: 0 0 calc(50% - 8px); min-width: 0; background: #fff; border: 1px solid #e8e8e8; border-radius: 4px; overflow: hidden; }}
  .profile-header {{ background: #f5f5f5; padding: 8px 12px; font-size: 12px; font-weight: 600; color: #555; border-bottom: 1px solid #e8e8e8; }}
  .profile-empty {{ padding: 12px; color: #999; font-size: 12px; font-style: italic; }}
  .stack-row {{ display: flex; align-items: baseline; padding: 4px 12px; border-bottom: 1px solid #f5f5f5; font-size: 11px; font-family: 'SF Mono', 'Consolas', monospace; min-width: 0; }}
  .stack-row:last-child {{ border-bottom: none; }}
  .stack-row:hover {{ background: #f8f9fa; }}
  .stack-bytes {{ flex: 0 0 80px; text-align: right; padding-right: 8px; font-weight: 600; color: #333; white-space: nowrap; }}
  .stack-bar-wrap {{ flex: 0 0 60px; height: 10px; background: #f0f0f0; border-radius: 2px; overflow: hidden; margin-right: 8px; }}
  .stack-bar {{ height: 100%; background: #64b5f6; border-radius: 2px; }}
  .stack-trace {{ flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #666; }}
  .stack-trace:hover {{ white-space: normal; overflow-wrap: break-word; }}
  .footer {{ margin-top: 16px; font-size: 12px; color: #999; }}
</style>
</head>
<body>

<h1>Parser AST Memory Check Report</h1>
<p class="subtitle">Measuring AST allocated memory during SQL parsing (not query execution). Direction: Master &rarr; PR.</p>

<div class="summary">
  <div class="summary-item">
    <span class="summary-label">Status</span>
    <span class="summary-value {'fail' if total_regressions > 0 else 'ok'}">{overall_status}</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Queries</span>
    <span class="summary-value">{num_queries}</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Master Total</span>
    <span class="summary-value">{total_master:,} B</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">PR Total</span>
    <span class="summary-value">{total_pr:,} B</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Change</span>
    <span class="summary-value {'fail' if total_change > 0 else 'ok'}">{total_change:+,} B</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Regressions</span>
    <span class="summary-value {'fail' if total_regressions > 0 else ''}">{total_regressions}</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Improvements</span>
    <span class="summary-value">{total_improvements}</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Duration</span>
    <span class="summary-value">{duration:.1f}s</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Threshold</span>
    <span class="summary-value">&plusmn;{CHANGE_THRESHOLD_BYTES} B</span>
  </div>
</div>

<table id="reportTable">
  <thead>
    <tr>
      <th onclick="sortTable(0, 'num')" style="width:50px"># <span class="sort-arrow">&#9650;&#9660;</span></th>
      <th onclick="sortTable(1, 'str')">Query <span class="sort-arrow">&#9650;&#9660;</span></th>
      <th onclick="sortTable(2, 'num')" style="width:100px">Master (B) <span class="sort-arrow">&#9650;&#9660;</span></th>
      <th onclick="sortTable(3, 'num')" style="width:100px">PR (B) <span class="sort-arrow">&#9650;&#9660;</span></th>
      <th onclick="sortTable(4, 'num')" style="width:100px">Change (B) <span class="sort-arrow">&#9650;&#9660;</span></th>
      <th onclick="sortTable(5, 'str')" style="width:70px">Status <span class="sort-arrow">&#9650;&#9660;</span></th>
    </tr>
  </thead>
  <tbody>
{rows_html}  </tbody>
</table>

<p class="footer">Click any row to expand details with full memory profiles and stack traces. Click column headers to sort.</p>

<script>
function toggleDetail(row) {{
  var detail = row.nextElementSibling;
  if (detail && detail.classList.contains('detail-row')) {{
    detail.style.display = detail.style.display === 'none' ? '' : 'none';
  }}
}}

var sortState = {{}};
function sortTable(colIdx, type) {{
  var table = document.getElementById('reportTable');
  var tbody = table.tBodies[0];
  var pairs = [];
  var rows = Array.from(tbody.rows);
  for (var i = 0; i < rows.length; i += 2) {{
    pairs.push([rows[i], rows[i+1]]);
  }}
  var dir = sortState[colIdx] === 'asc' ? 'desc' : 'asc';
  sortState[colIdx] = dir;
  pairs.sort(function(a, b) {{
    var aVal = a[0].cells[colIdx].textContent.trim().replace(/,/g, '');
    var bVal = b[0].cells[colIdx].textContent.trim().replace(/,/g, '');
    if (type === 'num') {{
      aVal = parseFloat(aVal) || 0;
      bVal = parseFloat(bVal) || 0;
    }}
    if (aVal < bVal) return dir === 'asc' ? -1 : 1;
    if (aVal > bVal) return dir === 'asc' ? 1 : -1;
    return 0;
  }});
  for (var j = 0; j < pairs.length; j++) {{
    tbody.appendChild(pairs[j][0]);
    tbody.appendChild(pairs[j][1]);
  }}
}}
</script>

</body>
</html>"""

    with open(output_path, "w") as f:
        f.write(html)
    print(f"HTML report written to: {output_path}")


def run_profiler_collect_heap(
    binary_path: str, query: str, profile_prefix: str
) -> dict:
    """
    Run parser_memory_profiler with heap profiling on a single query.
    Does NOT symbolize — heap files are collected for batch symbolization later.
    Returns dict with keys: jemalloc_diff, heap_before, heap_after, error
    """
    env = os.environ.copy()
    malloc_conf = "prof:true,prof_active:true,prof_thread_active_init:true,lg_prof_sample:0"
    # macOS: jemalloc uses je_ prefix -> JE_MALLOC_CONF
    # Linux: jemalloc without prefix -> MALLOC_CONF
    # Set both to be safe (only the matching one takes effect)
    env["JE_MALLOC_CONF"] = malloc_conf
    env["MALLOC_CONF"] = malloc_conf

    args = [binary_path, "--profile", profile_prefix]

    try:
        result = subprocess.run(
            args,
            input=query,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}

    if result.returncode != 0:
        return {"error": f"exit code {result.returncode}: {result.stderr[:200]}"}

    # Parse stdout: query_length \t before \t after \t diff
    parts = result.stdout.strip().split("\t")
    jemalloc_diff = 0
    if len(parts) == 4:
        jemalloc_diff = int(parts[3])

    # Parse stderr to find heap profile file paths
    heap_before = ""
    heap_after = ""
    for line in result.stderr.split("\n"):
        if line.startswith("Profile before: "):
            heap_before = line.split(": ", 1)[1].strip()
        elif line.startswith("Profile after:"):
            heap_after = line.split(":", 1)[1].strip().lstrip()

    return {
        "jemalloc_diff": jemalloc_diff,
        "heap_before": heap_before,
        "heap_after": heap_after,
        "error": None,
    }


def batch_symbolize(binary_path: str, heap_files: list) -> bool:
    """
    Run batch symbolization: collects all unique addresses from all heap files,
    symbolizes once, writes .heap.sym for each.
    Returns True on success.
    """
    if not heap_files:
        return True

    args = [binary_path, "--symbolize-batch"] + heap_files

    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=600,  # batch may take longer
        )
    except subprocess.TimeoutExpired:
        print("ERROR: batch symbolization timed out")
        return False
    except Exception as e:
        print(f"ERROR: batch symbolization failed: {e}")
        return False

    if result.returncode != 0:
        print(f"ERROR: batch symbolization exit code {result.returncode}: {result.stderr[:500]}")
        return False

    print(result.stderr.rstrip())
    return True


def analyze_heap_profiles(heap_before: str, heap_after: str) -> dict:
    """
    Parse symbolized heap profiles and compute diff for a single query.
    Expects .heap.sym files to exist (from batch symbolization).
    Returns dict with: heap_diff, stack_diffs
    """
    sym_before = heap_before + ".sym" if heap_before else ""
    sym_after = heap_after + ".sym" if heap_after else ""

    heap_diff = 0
    stack_diffs = []
    if sym_before and sym_after:
        stacks_before = parse_symbolized_heap(sym_before)
        stacks_after = parse_symbolized_heap(sym_after)
        heap_diff, stack_diffs = compute_diff(stacks_before, stacks_after)

    return {"heap_diff": heap_diff, "stack_diffs": stack_diffs}


# Keep the old single-file API for backward compatibility (e.g. local testing)
def run_profiler_with_heap(
    binary_path: str, query: str, profile_prefix: str, symbolize: bool = True
) -> dict:
    """
    Run parser_memory_profiler with heap profiling on a single query.
    Optionally symbolizes inline (slower for many queries — prefer batch_symbolize).
    Returns dict with keys: jemalloc_diff, heap_diff, stack_diffs, error
    """
    env = os.environ.copy()
    malloc_conf = "prof:true,prof_active:true,prof_thread_active_init:true,lg_prof_sample:0"
    env["JE_MALLOC_CONF"] = malloc_conf
    env["MALLOC_CONF"] = malloc_conf

    args = [binary_path, "--profile", profile_prefix]
    if symbolize:
        args.append("--symbolize")

    try:
        result = subprocess.run(
            args,
            input=query,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}

    if result.returncode != 0:
        return {"error": f"exit code {result.returncode}: {result.stderr[:200]}"}

    # Parse stdout: query_length \t before \t after \t diff
    parts = result.stdout.strip().split("\t")
    jemalloc_diff = 0
    if len(parts) == 4:
        jemalloc_diff = int(parts[3])

    # Parse stderr to find symbolized file paths
    sym_before = ""
    sym_after = ""
    for line in result.stderr.split("\n"):
        if line.startswith("Symbolized before: "):
            sym_before = line.split(": ", 1)[1].strip()
        elif line.startswith("Symbolized after: "):
            sym_after = line.split(": ", 1)[1].strip()

    # Parse symbolized heap profiles and compute diff
    heap_diff = 0
    stack_diffs = []
    if sym_before and sym_after:
        stacks_before = parse_symbolized_heap(sym_before)
        stacks_after = parse_symbolized_heap(sym_after)
        heap_diff, stack_diffs = compute_diff(stacks_before, stacks_after)

    return {
        "jemalloc_diff": jemalloc_diff,
        "heap_diff": heap_diff,
        "stack_diffs": stack_diffs,
        "error": None,
    }


def load_queries(queries_file: str) -> list:
    """Load queries from file, skip empty lines and comments."""
    with open(queries_file, "r") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]


def main():
    stop_watch = Utils.Stopwatch()
    results = []

    pr_profiler = f"{TEMP_DIR}/parser_memory_profiler"
    master_profiler = f"{TEMP_DIR}/parser_memory_profiler_master"
    profiles_dir = f"{TEMP_DIR}/profiles"

    # Check PR binary exists (downloaded by praktika from artifact)
    if not Path(pr_profiler).exists():
        results.append(
            Result(
                name="Check PR binary",
                status=Result.Status.FAILED,
                info=f"PR binary not found at {pr_profiler}",
            )
        )
        Result.create_from(results=results, stopwatch=stop_watch).complete_job()
        return

    Shell.check(f"chmod +x {pr_profiler}")
    results.append(Result(name="Check PR binary", status=Result.Status.SUCCESS))

    # Download master binary
    if download_master_binary(master_profiler):
        results.append(
            Result(name="Download master binary", status=Result.Status.SUCCESS)
        )
    else:
        results.append(
            Result(
                name="Download master binary",
                status=Result.Status.FAILED,
                info="Failed to download master binary from S3",
            )
        )
        Result.create_from(results=results, stopwatch=stop_watch).complete_job()
        return

    # Load queries
    queries = load_queries(QUERIES_FILE)
    print(f"Loaded {len(queries)} queries from {QUERIES_FILE}")

    # Create profiles directory
    os.makedirs(profiles_dir, exist_ok=True)

    # =========================================================================
    # Phase 1: Profile all queries (no symbolization — fast)
    # =========================================================================
    print("Phase 1: Profiling all queries...")
    raw_results = []  # (query_num, query, query_display, master_data, pr_data)
    master_heap_files = []
    pr_heap_files = []

    for i, query in enumerate(queries):
        query_num = i + 1
        query_display = query[:60].replace("\n", " ").replace("\t", " ")

        # Run master profiler (no symbolize)
        master_prefix = f"{profiles_dir}/q{query_num}_master_"
        master_data = run_profiler_collect_heap(
            master_profiler, query, master_prefix
        )

        # Run PR profiler (no symbolize)
        pr_prefix = f"{profiles_dir}/q{query_num}_pr_"
        pr_data = run_profiler_collect_heap(
            pr_profiler, query, pr_prefix
        )

        raw_results.append((query_num, query, query_display, master_data, pr_data))

        # Collect heap file paths for batch symbolization
        if not master_data.get("error"):
            if master_data.get("heap_before"):
                master_heap_files.append(master_data["heap_before"])
            if master_data.get("heap_after"):
                master_heap_files.append(master_data["heap_after"])
        if not pr_data.get("error"):
            if pr_data.get("heap_before"):
                pr_heap_files.append(pr_data["heap_before"])
            if pr_data.get("heap_after"):
                pr_heap_files.append(pr_data["heap_after"])

        if query_num % 10 == 0:
            print(f"  Profiled {query_num}/{len(queries)} queries...")

    # =========================================================================
    # Phase 2: Batch symbolization (single DWARF pass per binary)
    # =========================================================================
    print(f"\nPhase 2: Batch symbolizing {len(master_heap_files)} master + {len(pr_heap_files)} PR heap files...")

    sym_ok = True
    if master_heap_files:
        if not batch_symbolize(master_profiler, master_heap_files):
            print("WARNING: Master batch symbolization failed, stacks will be empty")
            sym_ok = False
    if pr_heap_files:
        if not batch_symbolize(pr_profiler, pr_heap_files):
            print("WARNING: PR batch symbolization failed, stacks will be empty")
            sym_ok = False

    if sym_ok:
        results.append(Result(name="Batch symbolization", status=Result.Status.SUCCESS))
    else:
        results.append(
            Result(name="Batch symbolization", status=Result.Status.FAILED, info="See logs")
        )

    # =========================================================================
    # Phase 3: Analyze symbolized profiles and build results
    # =========================================================================
    print("\nPhase 3: Analyzing profiles and building report...")

    ci_results = []  # Result objects for CI report
    html_data = []  # structured dicts for HTML report
    total_regressions = 0
    total_improvements = 0
    total_master_bytes = 0
    total_pr_bytes = 0

    for query_num, query, query_display, master_data, pr_data in raw_results:
        if master_data.get("error") or pr_data.get("error"):
            error_info = f"Query: {query}\n\nmaster: {master_data.get('error', 'ok')}, pr: {pr_data.get('error', 'ok')}"
            ci_results.append(
                Result(
                    name=f"Query {query_num}",
                    status=Result.Status.ERROR,
                    info=error_info,
                )
            )
            html_data.append({
                "num": query_num,
                "query": query,
                "query_display": query_display,
                "master_bytes": 0,
                "pr_bytes": 0,
                "change": 0,
                "status": "ERROR",
                "master_stacks": [],
                "pr_stacks": [],
            })
            continue

        # Analyze symbolized heap profiles
        master_analysis = analyze_heap_profiles(
            master_data.get("heap_before", ""), master_data.get("heap_after", "")
        )
        pr_analysis = analyze_heap_profiles(
            pr_data.get("heap_before", ""), pr_data.get("heap_after", "")
        )

        master_bytes = master_analysis["heap_diff"]
        pr_bytes = pr_analysis["heap_diff"]
        change = pr_bytes - master_bytes

        total_master_bytes += master_bytes
        total_pr_bytes += pr_bytes

        # Determine status
        if change >= CHANGE_THRESHOLD_BYTES:
            status = "FAIL"
            total_regressions += 1
        elif change <= -CHANGE_THRESHOLD_BYTES:
            status = "OK"  # improvement is OK
            total_improvements += 1
        else:
            status = "OK"

        master_stacks = master_analysis.get("stack_diffs", [])
        pr_stacks = pr_analysis.get("stack_diffs", [])

        # Build info string with full query and stack profiles for both versions
        info_lines = [
            f"Query: {query}",
            f"\nAST allocation diff (heap profile): master={master_bytes:,} bytes, PR={pr_bytes:,} bytes, change={change:+,} bytes",
        ]

        if change >= CHANGE_THRESHOLD_BYTES:
            info_lines.append(f"\nRegression: +{change:,} bytes")
        elif change <= -CHANGE_THRESHOLD_BYTES:
            info_lines.append(f"\nImprovement: {change:,} bytes")

        # Always include full profiles for both versions
        info_lines.append(f"\n--- Master profile ({master_bytes:,} bytes) ---")
        if master_stacks:
            for diff_bytes, stack in master_stacks:
                info_lines.append(f"  {diff_bytes:+,} bytes | {stack}")
        else:
            info_lines.append("  (no stack data)")

        info_lines.append(f"\n--- PR profile ({pr_bytes:,} bytes) ---")
        if pr_stacks:
            for diff_bytes, stack in pr_stacks:
                info_lines.append(f"  {diff_bytes:+,} bytes | {stack}")
        else:
            info_lines.append("  (no stack data)")

        ci_results.append(
            Result(
                name=f"Query {query_num}",
                status=status,
                info="\n".join(info_lines),
            )
        )

        html_data.append({
            "num": query_num,
            "query": query,
            "query_display": query_display,
            "master_bytes": master_bytes,
            "pr_bytes": pr_bytes,
            "change": change,
            "status": status,
            "master_stacks": master_stacks,
            "pr_stacks": pr_stacks,
        })

    # Clean up heap profile files to save disk
    for f in glob.glob(f"{profiles_dir}/*.heap*"):
        try:
            os.remove(f)
        except OSError:
            pass

    # Generate standalone HTML report
    html_report_path = f"{TEMP_DIR}/parser_memory_report.html"
    generate_html_report(
        html_data,
        total_master_bytes,
        total_pr_bytes,
        total_regressions,
        total_improvements,
        stop_watch.duration,
        html_report_path,
    )

    # Create "Tests" sub-result for CI report
    tests_status = Result.Status.SUCCESS
    if total_regressions > 0:
        tests_status = Result.Status.FAILED

    total_change = total_pr_bytes - total_master_bytes
    tests_info = (
        f"Queries: {len(queries)}, "
        f"Master total: {total_master_bytes:,} bytes, "
        f"PR total: {total_pr_bytes:,} bytes, "
        f"Change: {total_change:+,} bytes"
    )
    if total_regressions > 0:
        tests_info += f", Regressions: {total_regressions}"
    if total_improvements > 0:
        tests_info += f", Improvements: {total_improvements}"

    tests_result = Result.create_from(
        name="Tests",
        results=ci_results,
        status=tests_status,
        info=tests_info,
    )
    results.append(tests_result)

    # Attach HTML report as a file (uploaded to S3, shown as link in CI)
    report_files = []
    if Path(html_report_path).exists():
        report_files.append(html_report_path)

    Result.create_from(
        results=results, stopwatch=stop_watch, files=report_files
    ).complete_job()


if __name__ == "__main__":
    main()
