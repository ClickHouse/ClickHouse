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

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

TEMP_DIR = f"{Utils.cwd()}/ci/tmp"
QUERIES_FILE = f"{Utils.cwd()}/utils/parser-memory-profiler/test_queries.txt"
MASTER_PROFILER_BASE_URL = "https://clickhouse-builds.s3.us-east-1.amazonaws.com"

# Threshold: a change is significant only if BOTH conditions are met
CHANGE_THRESHOLD_BYTES = 100
CHANGE_THRESHOLD_PCT = 1.0

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
)


def get_merge_base_profiler_url() -> str:
    """Find the S3 URL for a recent master parser_memory_profiler binary.
    Iterates master_track_commits_sha (like performance_tests.py) to find
    the closest available build. Returns empty string if none found."""
    try:
        info = Info()
        commits = info.get_kv_data("master_track_commits_sha") or []
        for sha in commits:
            if not re.fullmatch(r"[0-9a-f]{40}", sha):
                continue
            url = f"{MASTER_PROFILER_BASE_URL}/REFs/master/{sha}/build_arm_binary/parser_memory_profiler"
            if Shell.check(f"curl -sfI '{url}' > /dev/null"):
                print(f"Using master binary from commit {sha[:12]}")
                return url
        print(f"No master binary found in {len(commits)} tracked commits")
    except Exception as e:
        print(f"Could not resolve master binary: {e}")
    return ""


def download_master_binary(dest_path: str) -> str:
    """Download a master parser_memory_profiler binary from S3.
    Returns error message on failure, empty string on success."""
    url = get_merge_base_profiler_url()
    if not url:
        return "No master binary found in tracked commits — cannot compare"
    print(f"Downloading base binary from {url}")
    if not Shell.check(f"wget -nv -O '{dest_path}' '{url}' && chmod +x '{dest_path}'"):
        return f"Failed to download master binary from {url}"
    return ""


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

    Returns dict: { "stack_key" -> { "bytes": int, "frames": ["sym1", "sym2", ...] } }
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
    in_heap_section = False

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
    Returns (total_diff, list of (diff_bytes, formatted_stack, full_frames) sorted by |diff| desc).
    full_frames is the untruncated frame list for accurate cross-version matching.
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
                continue
            full = flatten_frames_full(frames)
            diffs.append((diff, formatted, full))
            total += diff

    diffs.sort(key=lambda x: -abs(x[0]))
    return total, diffs


def flatten_frames_full(frames: list) -> list:
    """Filter noise frames, expand inline frames (--), without truncation.
    Keeps unsymbolized addresses as placeholders to preserve stack uniqueness."""
    filtered = filter_stack_frames(frames)
    flat = []
    for f in filtered:
        for part in f.split("--"):
            if part == "??":
                continue
            flat.append(part)
    return flat


def compute_cross_version_diff(master_stacks: list, pr_stacks: list) -> list:
    """Compute per-stack diff between master and PR allocation profiles.
    Input: lists of (diff_bytes, formatted_stack_str, full_frames).
    Uses full (untruncated) frames as map keys to avoid false merges,
    but returns the formatted (truncated) string for display.
    Returns list of (delta, master_bytes, pr_bytes, stack_str) sorted by |delta| desc."""
    master_map = {}
    master_display = {}
    for b, s, full in master_stacks:
        key = tuple(full)
        master_map[key] = master_map.get(key, 0) + b
        master_display[key] = s
    pr_map = {}
    pr_display = {}
    for b, s, full in pr_stacks:
        key = tuple(full)
        pr_map[key] = pr_map.get(key, 0) + b
        pr_display[key] = s
    all_keys = set(master_map.keys()) | set(pr_map.keys())
    diffs = []
    for key in all_keys:
        m = master_map.get(key, 0)
        p = pr_map.get(key, 0)
        delta = p - m
        if delta != 0:
            display = pr_display.get(key) or master_display.get(key, " > ".join(key))
            diffs.append((delta, m, p, display))
    diffs.sort(key=lambda x: -abs(x[0]))
    return diffs


def format_cross_diff_html(cross_diff: list) -> str:
    """Render cross-version diff as an HTML table."""
    if not cross_diff:
        return '<div class="diff-empty">(identical allocation profiles)</div>'
    rows = ""
    for delta, m_bytes, p_bytes, stack in cross_diff:
        color = "#c62828" if delta > 0 else "#2e7d32"
        rows += (
            f'<tr>'
            f'<td class="diff-bytes" style="color:{color}">{delta:+,}</td>'
            f'<td class="diff-bytes">{m_bytes:+,}</td>'
            f'<td class="diff-bytes">{p_bytes:+,}</td>'
            f'<td class="diff-stack">{html_escape(stack)}</td>'
            f'</tr>'
        )
    return (
        f'<table class="diff-table">'
        f'<thead><tr><th style="width:90px">Delta (B)</th><th style="width:90px">Master (B)</th>'
        f'<th style="width:90px">PR (B)</th><th>Stack</th></tr></thead>'
        f'<tbody>{rows}</tbody></table>'
    )


def _build_flame_tree(collapsed: list) -> dict:
    """Build a tree from collapsed stacks: list of (stack_str, bytes)."""
    root = {"n": "all", "v": 0, "c": {}, "self": 0}
    for stack_str, byte_val in collapsed:
        parts = stack_str.split(";")
        root["v"] += byte_val
        node = root
        for p in parts:
            if p not in node["c"]:
                node["c"][p] = {"n": p, "v": 0, "c": {}, "self": 0}
            node = node["c"][p]
            node["v"] += byte_val
        node["self"] += byte_val
    return root


def _build_diff_flame_tree(master_collapsed: list, pr_collapsed: list) -> dict:
    """Build a merged tree with master_v and pr_v for diff coloring.
    Width is based on max(master, pr). Color encodes the delta."""
    root = {"n": "all", "mv": 0, "pv": 0, "c": {}, "ms": 0, "ps": 0}

    def _add(collapsed, m_key, s_key):
        for stack_str, byte_val in collapsed:
            parts = stack_str.split(";")
            root[m_key] += byte_val
            node = root
            for p in parts:
                if p not in node["c"]:
                    node["c"][p] = {"n": p, "mv": 0, "pv": 0, "c": {}, "ms": 0, "ps": 0}
                node = node["c"][p]
                node[m_key] += byte_val
            node[s_key] += byte_val

    _add(master_collapsed, "mv", "ms")
    _add(pr_collapsed, "pv", "ps")
    return root


def generate_diff_flamegraph_svg(
    master_collapsed: list, pr_collapsed: list, max_levels: int = 25
) -> str:
    """Generate a differential flamegraph SVG.
    Width = max(master, PR) per node. Color = red (regression), blue (improvement), grey (same)."""
    if not master_collapsed and not pr_collapsed:
        return ""
    root = _build_diff_flame_tree(master_collapsed, pr_collapsed)
    total = max(root["mv"], root["pv"])
    if total == 0:
        return ""

    W = 1200
    RH = 18
    PAD = 0.5

    def _trim(nd, d):
        if d >= max_levels:
            nd["ms"] = nd["mv"]
            nd["ps"] = nd["pv"]
            nd["c"] = {}
            return
        for child in nd["c"].values():
            _trim(child, d + 1)

    _trim(root, 0)

    max_depth = [0]

    def _depth(nd, d):
        max_depth[0] = max(max_depth[0], d)
        for child in nd["c"].values():
            _depth(child, d + 1)

    _depth(root, 0)
    H = (max_depth[0] + 1) * RH + 4

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}">'
    ]

    def _color(nd):
        delta = nd["pv"] - nd["mv"]
        if delta == 0:
            return "#d0d0d0"
        mx = max(nd["mv"], nd["pv"])
        ratio = min(abs(delta) / mx, 1.0) if mx else 0
        intensity = int(60 + ratio * 140)
        if delta > 0:
            return f"rgb({intensity},60,60)"
        return f"rgb(60,60,{intensity})"

    def _draw(nd, x, w, d, parent_max):
        if w < 0.5:
            return
        y = H - (d + 1) * RH - 2
        rw = w - PAD
        if rw < 0.3:
            return
        col = "#e0e0e0" if d == 0 else _color(nd)
        esc_name = _svg_escape(nd["n"])
        delta = nd["pv"] - nd["mv"]
        sign = "+" if delta > 0 else ""
        tip = f"{esc_name} | master: {nd['mv']:,} B | PR: {nd['pv']:,} B | delta: {sign}{delta:,} B"
        if nd["ms"] > 0 or nd["ps"] > 0:
            s_delta = nd["ps"] - nd["ms"]
            s_sign = "+" if s_delta > 0 else ""
            tip += f" | self: m={nd['ms']:,} p={nd['ps']:,} ({s_sign}{s_delta:,})"
        parts.append(
            f'<rect x="{x:.2f}" y="{y}" width="{rw:.2f}" height="{RH - 1}" '
            f'fill="{col}" rx="1">'
            f"<title>{tip}</title></rect>"
        )
        if rw > 40:
            max_chars = int(rw / 6.5)
            label = nd["n"]
            if len(label) > max_chars:
                label = label[: max_chars - 1] + ".."
            text_col = "#fff" if d > 0 and abs(delta) > max(nd["mv"], nd["pv"]) * 0.3 else "#333"
            parts.append(
                f'<text x="{x + 2:.2f}" y="{y + 13}" fill="{text_col}" '
                f'style="font:9px monospace;pointer-events:none">'
                f"{_svg_escape(label)}</text>"
            )
        cx = x
        nd_max = max(nd["mv"], nd["pv"])
        sorted_children = sorted(nd["c"].values(), key=lambda c: -max(c["mv"], c["pv"]))
        for child in sorted_children:
            c_max = max(child["mv"], child["pv"])
            cw = (c_max / nd_max) * w if nd_max else 0
            _draw(child, cx, cw, d + 1, nd_max)
            cx += cw

    _draw(root, 0, W, 0, total)
    parts.append("</svg>")
    return "\n".join(parts)


def _svg_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def generate_flamegraph_svg(collapsed: list, max_levels: int = 25) -> str:
    """Generate an SVG icicle flamegraph from collapsed stacks.
    Truncates tree at max_levels to keep the chart readable."""
    if not collapsed:
        return ""
    root = _build_flame_tree(collapsed)
    if root["v"] == 0:
        return ""

    W = 1200
    RH = 18
    PAD = 0.5
    hues = [210, 190, 170, 150, 130, 40, 20, 0]

    def _trim(nd, d):
        if d >= max_levels:
            nd["self"] = nd["v"]
            nd["c"] = {}
            return
        for child in nd["c"].values():
            _trim(child, d + 1)

    _trim(root, 0)

    max_depth = [0]

    def _depth(nd, d):
        max_depth[0] = max(max_depth[0], d)
        for child in nd["c"].values():
            _depth(child, d + 1)

    _depth(root, 0)
    H = (max_depth[0] + 1) * RH + 4

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}">'
    ]

    def _hsl(d):
        h = hues[d % len(hues)]
        return f"hsl({h},55%,{72 + ((d * 3) % 12)}%)"

    def _draw(nd, x, w, d):
        if w < 0.5:
            return
        y = H - (d + 1) * RH - 2
        rw = w - PAD
        if rw < 0.3:
            return
        col = "#e0e0e0" if d == 0 else _hsl(d)
        esc_name = _svg_escape(nd["n"])
        tip = f"{esc_name} | total: {nd['v']:,} B"
        if nd["self"] > 0:
            tip += f" | self: {nd['self']:,} B"
        parts.append(
            f'<rect x="{x:.2f}" y="{y}" width="{rw:.2f}" height="{RH - 1}" '
            f'fill="{col}" rx="1">'
            f"<title>{tip}</title></rect>"
        )
        if rw > 40:
            max_chars = int(rw / 6.5)
            label = nd["n"]
            if len(label) > max_chars:
                label = label[: max_chars - 1] + ".."
            parts.append(
                f'<text x="{x + 2:.2f}" y="{y + 13}" fill="#333" '
                f'style="font:9px monospace;pointer-events:none">'
                f"{_svg_escape(label)}</text>"
            )
        cx = x
        sorted_children = sorted(nd["c"].values(), key=lambda c: -c["v"])
        for child in sorted_children:
            cw = (child["v"] / nd["v"]) * w if nd["v"] else 0
            _draw(child, cx, cw, d + 1)
            cx += cw

    _draw(root, 0, W, 0)
    parts.append("</svg>")
    return "\n".join(parts)


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
    for entry in stacks:
        diff_bytes, stack = entry[0], entry[1]
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
    query_results, total_master, total_pr, total_regressions, total_improvements, total_errors, duration, output_path
):
    """Generate a standalone HTML report viewable in browser.
    Detail panels live OUTSIDE the table to avoid table-layout issues with SVG/nested tables."""
    total_change = total_pr - total_master
    overall_status = "FAIL" if (total_regressions > 0 or total_errors > 0) else "OK"
    num_queries = len(query_results)

    rows_html = ""

    for r in query_results:
        change = r["change"]
        status = r["status"]
        qid = r["num"]

        master_b = r["master_bytes"]
        abs_ch = abs(change)
        base_b = abs(master_b)
        pct_ch = (abs_ch / base_b * 100) if base_b > 0 else (100.0 if abs_ch > 0 else 0)
        sig = abs_ch > CHANGE_THRESHOLD_BYTES and pct_ch > CHANGE_THRESHOLD_PCT

        if status == Result.StatusExtended.FAIL:
            row_class = "regression"
            status_badge = '<span class="badge badge-fail">FAIL</span>'
        elif status == Result.StatusExtended.ERROR:
            row_class = "error"
            status_badge = '<span class="badge badge-error">ERROR</span>'
        elif sig and change < 0:
            row_class = "improvement"
            status_badge = '<span class="badge badge-ok">OK</span>'
        else:
            row_class = ""
            status_badge = '<span class="badge badge-ok">OK</span>'

        change_str = f"{change:+,}" if change != 0 else "0"
        pct_str = f"{pct_ch:+.1f}%" if change != 0 else "0.0%"
        if change < 0:
            pct_str = f"-{pct_ch:.1f}%"
        query_escaped = html_escape(r["query"])

        master_profile = format_profile_html(
            r.get("master_stacks", []), r["master_bytes"], "Master profile"
        )
        pr_profile = format_profile_html(
            r.get("pr_stacks", []), r["pr_bytes"], "PR profile"
        )
        cross_diff_html = format_cross_diff_html(r.get("cross_diff", []))
        flame_svg = generate_flamegraph_svg(r.get("collapsed_pr", []))
        flame_html = (
            f'<div class="flame-container">{flame_svg}</div>'
            if flame_svg
            else '<div class="flame-placeholder">(no allocation data for flamegraph)</div>'
        )
        diff_flame_svg = generate_diff_flamegraph_svg(
            r.get("collapsed_master", []), r.get("collapsed_pr", [])
        )
        diff_flame_html = (
            f'<div class="flame-container">{diff_flame_svg}</div>'
            if diff_flame_svg
            else '<div class="flame-placeholder">(no data for diff flamegraph)</div>'
        )

        rows_html += (
            f'<tr class="{row_class}" onclick="toggleDetail({qid})">'
            f"<td>{qid}</td>"
            f'<td class="query-cell" title="{query_escaped}">{html_escape(r["query_display"])}</td>'
            f'<td class="num">{r["master_bytes"]:,}</td>'
            f'<td class="num">{r["pr_bytes"]:,}</td>'
            f'<td class="num">{change_str}</td>'
            f'<td class="num">{pct_str}</td>'
            f"<td>{status_badge}</td>"
            f"</tr>\n"
            f'<tr class="detail-row" id="detail-{qid}" style="display:none">'
            f'<td colspan="7"><div class="detail-content">'
            f'<div class="detail-query"><strong>Query {qid}:</strong> <code>{query_escaped}</code></div>'
            f'<div class="detail-summary">'
            f'<span>Master: <strong>{r["master_bytes"]:,}</strong> B</span>'
            f'<span>PR: <strong>{r["pr_bytes"]:,}</strong> B</span>'
            f'<span>Change: <strong>{change_str}</strong> B ({pct_str})</span>'
            f"</div>"
            f'<div class="section-label">Cross-version diff (PR &minus; Master)</div>'
            f"{cross_diff_html}"
            f'<div class="section-label">Diff flamegraph (red = regression, blue = improvement)</div>'
            f"{diff_flame_html}"
            f'<div class="section-label">PR allocation flamegraph</div>'
            f"{flame_html}"
            f'<div class="section-label">Individual profiles</div>'
            f'<div class="profiles-container">{master_profile}{pr_profile}</div>'
            f"</div></td></tr>\n"
        )

    # Use doubled braces for CSS/JS in f-string
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Parser AST Memory Check Report</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #fafafa; color: #333; padding: 12px; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .subtitle {{ color: #666; font-size: 13px; margin-bottom: 20px; }}
  .summary {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; padding: 16px 20px; margin-bottom: 20px; display: flex; gap: 32px; flex-wrap: wrap; align-items: center; }}
  .summary-item {{ display: flex; flex-direction: column; }}
  .summary-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}
  .summary-value {{ font-size: 18px; font-weight: 600; }}
  .summary-value.ok {{ color: #388e3c; }}
  .summary-value.fail {{ color: #d32f2f; }}
  #reportTable {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; table-layout: fixed; }}
  th {{ background: #f5f5f5; border-bottom: 2px solid #e0e0e0; padding: 10px 12px; text-align: left; font-size: 12px; color: #555; text-transform: uppercase; letter-spacing: 0.5px; cursor: pointer; user-select: none; white-space: nowrap; overflow: hidden; }}
  th:hover {{ background: #eee; }}
  th .sort-arrow {{ font-size: 10px; margin-left: 4px; color: #aaa; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #f0f0f0; font-size: 13px; overflow: hidden; text-overflow: ellipsis; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-family: 'SF Mono', 'Consolas', monospace; white-space: nowrap; }}
  td.query-cell {{ font-family: 'SF Mono', 'Consolas', monospace; font-size: 12px; overflow: visible; white-space: normal; overflow-wrap: break-word; word-break: break-all; }}
  tr:hover {{ background: #f8f9fa; cursor: pointer; }}
  tr.regression {{ background: #fff5f5; }}
  tr.regression:hover {{ background: #ffebee; }}
  tr.improvement {{ background: #f1f8e9; }}
  tr.improvement:hover {{ background: #e8f5e9; }}
  tr.error {{ background: #fff8e1; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; }}
  .badge-ok {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-fail {{ background: #ffebee; color: #c62828; }}
  .badge-error {{ background: #fff8e1; color: #f57f17; }}
  .detail-row {{ background: #fff; }}
  .detail-row:hover {{ background: #fff; cursor: default; }}
  .detail-content {{ overflow: hidden; padding: 16px 20px; }}
  .detail-query {{ margin-bottom: 8px; font-size: 13px; }}
  .detail-query code {{ background: #f0f0f0; padding: 2px 6px; border-radius: 3px; font-size: 12px; word-break: break-all; }}
  .detail-summary {{ display: flex; gap: 24px; margin-bottom: 12px; font-size: 13px; color: #555; }}
  .detail-summary strong {{ color: #333; }}
  .profiles-container {{ display: flex; gap: 16px; overflow: hidden; }}
  .profile-section {{ flex: 1; min-width: 0; background: #f9f9f9; border: 1px solid #e8e8e8; border-radius: 4px; overflow: hidden; }}
  .profile-header {{ background: #f5f5f5; padding: 8px 12px; font-size: 12px; font-weight: 600; color: #555; border-bottom: 1px solid #e8e8e8; }}
  .profile-empty {{ padding: 12px; color: #999; font-size: 12px; font-style: italic; }}
  .stack-row {{ display: flex; align-items: baseline; padding: 4px 12px; border-bottom: 1px solid #f0f0f0; font-size: 11px; font-family: 'SF Mono', 'Consolas', monospace; }}
  .stack-row:last-child {{ border-bottom: none; }}
  .stack-row:hover {{ background: #f5f5f5; }}
  .stack-bytes {{ flex: 0 0 80px; text-align: right; padding-right: 8px; font-weight: 600; color: #333; white-space: nowrap; }}
  .stack-bar-wrap {{ flex: 0 0 60px; height: 10px; background: #f0f0f0; border-radius: 2px; overflow: hidden; margin-right: 8px; }}
  .stack-bar {{ height: 100%; background: #64b5f6; border-radius: 2px; }}
  .stack-trace {{ flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #666; }}
  .stack-trace:hover {{ white-space: normal; overflow-wrap: break-word; }}
  .section-label {{ font-size: 12px; font-weight: 600; color: #555; margin: 14px 0 6px; padding-bottom: 4px; border-bottom: 1px solid #e0e0e0; }}
  .diff-table {{ width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 11px; font-family: 'SF Mono', 'Consolas', monospace; background: #f9f9f9; border: 1px solid #e8e8e8; border-radius: 4px; margin-bottom: 8px; }}
  .diff-table th {{ background: #f5f5f5; padding: 6px 10px; text-align: left; font-size: 11px; color: #555; border-bottom: 1px solid #e0e0e0; white-space: nowrap; }}
  .diff-table td {{ padding: 4px 10px; border-bottom: 1px solid #f0f0f0; }}
  .diff-bytes {{ text-align: right; font-weight: 600; white-space: nowrap; }}
  .diff-stack {{ color: #555; overflow-wrap: break-word; word-break: break-all; }}
  .diff-empty {{ padding: 8px; color: #999; font-size: 12px; font-style: italic; }}
  .flame-container {{ background: #f9f9f9; border: 1px solid #e8e8e8; border-radius: 4px; overflow: hidden; margin-bottom: 8px; }}
  .flame-placeholder {{ padding: 12px; color: #999; font-size: 12px; font-style: italic; text-align: center; }}
  .flame-container svg {{ display: block; max-width: 100%; height: auto; }}
  .flame-container svg text {{ pointer-events: none; }}
  .flame-container svg rect:hover {{ stroke: #333; stroke-width: 1; }}
  .footer {{ margin-top: 16px; font-size: 12px; color: #999; }}
</style>
</head>
<body>

<h1>Parser AST Memory Check Report</h1>
<p class="subtitle">Measuring AST allocated memory during SQL parsing (not query execution). Direction: Master &rarr; PR.</p>

<div class="summary">
  <div class="summary-item">
    <span class="summary-label">Status</span>
    <span class="summary-value {'fail' if (total_regressions > 0 or total_errors > 0) else 'ok'}">{overall_status}</span>
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
    <span class="summary-value">{total_change:+,} B</span>
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
    <span class="summary-label">Errors</span>
    <span class="summary-value {'fail' if total_errors > 0 else ''}">{total_errors}</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Duration</span>
    <span class="summary-value">{duration:.1f}s</span>
  </div>
  <div class="summary-item">
    <span class="summary-label">Threshold</span>
    <span class="summary-value">&gt;{CHANGE_THRESHOLD_BYTES} B and &gt;{CHANGE_THRESHOLD_PCT}%</span>
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
      <th onclick="sortTable(5, 'num')" style="width:90px">Change (%) <span class="sort-arrow">&#9650;&#9660;</span></th>
      <th onclick="sortTable(6, 'str')" style="width:70px">Status <span class="sort-arrow">&#9650;&#9660;</span></th>
    </tr>
  </thead>
  <tbody>
{rows_html}  </tbody>
</table>

<p class="footer">Click any row to expand details. Click column headers to sort.</p>

<script>
function toggleDetail(qid) {{
  var row = document.getElementById('detail-' + qid);
  if (!row) return;
  row.style.display = row.style.display === 'none' ? '' : 'none';
}}

var sortState = {{}};
function sortTable(colIdx, type) {{
  var table = document.getElementById('reportTable');
  var tbody = table.tBodies[0];
  var dataRows = Array.from(tbody.rows).filter(function(r) {{
    return !r.classList.contains('detail-row');
  }});
  var dir = sortState[colIdx] === 'asc' ? 'desc' : 'asc';
  sortState[colIdx] = dir;
  dataRows.sort(function(a, b) {{
    var aVal = a.cells[colIdx].textContent.trim().replace(/,/g, '');
    var bVal = b.cells[colIdx].textContent.trim().replace(/,/g, '');
    if (type === 'num') {{
      aVal = parseFloat(aVal) || 0;
      bVal = parseFloat(bVal) || 0;
    }}
    if (aVal < bVal) return dir === 'asc' ? -1 : 1;
    if (aVal > bVal) return dir === 'asc' ? 1 : -1;
    return 0;
  }});
  for (var j = 0; j < dataRows.length; j++) {{
    tbody.appendChild(dataRows[j]);
    var detailId = dataRows[j].getAttribute('onclick').match(/\\d+/)[0];
    var detail = document.getElementById('detail-' + detailId);
    if (detail) tbody.appendChild(detail);
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

    if not heap_before or not heap_after:
        return {"error": f"heap profiles not found in profiler output (before={bool(heap_before)}, after={bool(heap_after)})"}
    if not os.path.exists(heap_before) or not os.path.exists(heap_after):
        return {"error": f"heap profile files missing on disk (before={os.path.exists(heap_before)}, after={os.path.exists(heap_after)})"}

    return {
        "jemalloc_diff": jemalloc_diff,
        "heap_before": heap_before,
        "heap_after": heap_after,
        "error": None,
    }


def batch_symbolize(binary_path: str, heap_files: list) -> bool:
    """
    Run batch symbolization: invokes --symbolize-batch on all heap files.
    The tool's global LRU cache deduplicates addresses across files.
    Writes .heap.sym for each input file.
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
            timeout=600,
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
    Returns dict with: heap_diff, stack_diffs, collapsed (for flamegraph)
    """
    sym_before = heap_before + ".sym" if heap_before else ""
    sym_after = heap_after + ".sym" if heap_after else ""

    heap_diff = 0
    stack_diffs = []
    collapsed = []
    collapsed_all = []

    if sym_before and sym_after:
        stacks_before = parse_symbolized_heap(sym_before)
        stacks_after = parse_symbolized_heap(sym_after)
        heap_diff, stack_diffs = compute_diff(stacks_before, stacks_after)

        all_keys = set(stacks_before.keys()) | set(stacks_after.keys())
        for key in all_keys:
            before_b = stacks_before.get(key, {}).get("bytes", 0)
            after_b = stacks_after.get(key, {}).get("bytes", 0)
            diff = after_b - before_b
            if diff == 0:
                continue
            frames = stacks_after.get(key, stacks_before.get(key, {})).get("frames", [])
            full = flatten_frames_full(frames)
            if not full:
                continue
            path = ";".join(reversed(full))
            collapsed_all.append((path, diff))
            if diff > 0:
                collapsed.append((path, diff))

    return {
        "heap_diff": heap_diff,
        "stack_diffs": stack_diffs,
        "collapsed": collapsed,
        "collapsed_all": collapsed_all,
    }


def load_queries(queries_file: str) -> list:
    """Load queries from file, skip empty lines and comments."""
    with open(queries_file, "r") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
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
    download_error = download_master_binary(master_profiler)
    if not download_error:
        results.append(
            Result(name="Download master binary", status=Result.Status.SUCCESS)
        )
    else:
        results.append(
            Result(
                name="Download master binary",
                status=Result.Status.FAILED,
                info=download_error,
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
        query_display = query.replace("\n", " ").replace("\t", " ")

        master_prefix = f"{profiles_dir}/q{query_num}_master_"
        master_data = run_profiler_collect_heap(
            master_profiler, query, master_prefix
        )

        pr_prefix = f"{profiles_dir}/q{query_num}_pr_"
        pr_data = run_profiler_collect_heap(
            pr_profiler, query, pr_prefix
        )

        raw_results.append((query_num, query, query_display, master_data, pr_data))

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
    total_errors = 0
    total_master_bytes = 0
    total_pr_bytes = 0

    for query_num, query, query_display, master_data, pr_data in raw_results:
        if master_data.get("error") or pr_data.get("error"):
            total_errors += 1
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
                "status": Result.StatusExtended.ERROR,
                "master_stacks": [],
                "pr_stacks": [],
                "cross_diff": [],
                "collapsed_pr": [],
                "collapsed_master": [],
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

        # Determine status: significant only if abs change > threshold AND > 1% of base.
        # For zero/negative baselines, use absolute threshold only.
        abs_change = abs(change)
        base = abs(master_bytes)
        pct_change = (abs_change / base * 100) if base > 0 else (100.0 if abs_change > 0 else 0)
        is_significant = abs_change > CHANGE_THRESHOLD_BYTES and pct_change > CHANGE_THRESHOLD_PCT

        if is_significant and change > 0:
            status = Result.StatusExtended.FAIL
            total_regressions += 1
        elif is_significant and change < 0:
            status = Result.StatusExtended.OK
            total_improvements += 1
        else:
            status = Result.StatusExtended.OK

        master_stacks = master_analysis.get("stack_diffs", [])
        pr_stacks = pr_analysis.get("stack_diffs", [])

        # Build info string with full query and stack profiles for both versions
        pct_str = f"{pct_change:.1f}%" if master_bytes else "N/A"
        info_lines = [
            f"Query: {query}",
            f"\nAST allocation diff (heap profile): master={master_bytes:,} bytes, PR={pr_bytes:,} bytes, change={change:+,} bytes ({pct_str})",
        ]

        if is_significant and change > 0:
            info_lines.append(f"\nRegression: +{change:,} bytes ({pct_change:.1f}%)")
        elif is_significant and change < 0:
            info_lines.append(f"\nImprovement: {change:,} bytes ({pct_change:.1f}%)")

        # Always include full profiles for both versions
        info_lines.append(f"\n--- Master profile ({master_bytes:,} bytes) ---")
        if master_stacks:
            for entry in master_stacks:
                info_lines.append(f"  {entry[0]:+,} bytes | {entry[1]}")
        else:
            info_lines.append("  (no stack data)")

        info_lines.append(f"\n--- PR profile ({pr_bytes:,} bytes) ---")
        if pr_stacks:
            for entry in pr_stacks:
                info_lines.append(f"  {entry[0]:+,} bytes | {entry[1]}")
        else:
            info_lines.append("  (no stack data)")

        ci_results.append(
            Result(
                name=f"Query {query_num}",
                status=status,
                info="\n".join(info_lines),
            )
        )

        cross_diff = compute_cross_version_diff(master_stacks, pr_stacks)

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
            "cross_diff": cross_diff,
            "collapsed_pr": pr_analysis.get("collapsed", []),
            "collapsed_master": master_analysis.get("collapsed", []),
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
        total_errors,
        stop_watch.duration,
        html_report_path,
    )

    # Create "Tests" sub-result for CI report
    tests_status = Result.Status.SUCCESS
    if total_regressions > 0 or total_errors > 0:
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
    if total_errors > 0:
        tests_info += f", Errors: {total_errors}"

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
