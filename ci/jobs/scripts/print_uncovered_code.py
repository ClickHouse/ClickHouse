import os
import sys
from ci.praktika.utils import Utils
import re
from collections import defaultdict
from ci.praktika.result import Result

repo_root = Utils.cwd()
temp_dir = f"{repo_root}/ci/tmp/"

# Lines matching any of these patterns are excluded from the uncovered report:
# they represent code paths that are intentionally never executed in normal runs
# (error paths, logical impossibilities, unreachable markers).
_NOISE_PATTERNS = [
    re.compile(r"\bLOGICAL_ERROR\b"),        # ErrorCodes::LOGICAL_ERROR (impossible conditions)
    re.compile(r"\bUNREACHABLE\s*\("),       # UNREACHABLE() macro
    re.compile(r"__builtin_unreachable\s*\("),
    re.compile(r"\babort\s*\(\s*\)"),        # abort()
    re.compile(r"\bstd::terminate\s*\("),    # std::terminate()
    re.compile(r"\babortOnFailedAssertion\s*\("),  # chassert failure handler
]

# Lazily-loaded source file cache: relpath -> list of raw lines
_source_cache: dict[str, list[str]] = {}


def _load_source(relpath: str) -> list[str]:
    if relpath not in _source_cache:
        abs_path = os.path.join(repo_root, relpath)
        try:
            with open(abs_path, encoding="utf-8", errors="replace") as f:
                _source_cache[relpath] = f.readlines()
        except FileNotFoundError:
            _source_cache[relpath] = []
    return _source_cache[relpath]


def _is_noise(relpath: str, lineno: int) -> bool:
    lines = _load_source(relpath)
    if not (1 <= lineno <= len(lines)):
        return False
    text = lines[lineno - 1].strip()
    return any(p.search(text) for p in _NOISE_PATTERNS)


def _normalize_sf(sf: str) -> str:
    """Strip machine-specific path prefix, keeping only the repo-relative path.

    Handles both CI paths (/home/ubuntu/actions-runner/_work/ClickHouse/ClickHouse/src/...)
    and local paths (/home/user/Documents/ClickHouse/src/...) by taking everything
    after the last occurrence of '/ClickHouse/'.
    """
    marker = "/ClickHouse/"
    idx = sf.rfind(marker)
    if idx >= 0:
        return sf[idx + len(marker):]
    return sf


def _parse_info(path: str) -> dict:
    """Parse an LCOV .info file.

    Returns dict mapping normalized-relpath -> {"lines": {lineno: count}, "fns": {name: count}}.
    """
    data: dict = {}
    cur: str | None = None
    cur_rel: str | None = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if line.startswith("SF:"):
                cur = line[3:]
                cur_rel = _normalize_sf(cur)
                data.setdefault(cur_rel, {"lines": {}, "fns": {}})
            elif not cur_rel:
                continue
            elif line.startswith("DA:"):
                parts = line[3:].split(",", 2)
                ln, cnt = int(parts[0]), int(parts[1])
                data[cur_rel]["lines"][ln] = data[cur_rel]["lines"].get(ln, 0) + cnt
            elif line.startswith("FNDA:"):
                rest = line[5:]
                cnt_str, name = rest.split(",", 1)
                data[cur_rel]["fns"][name] = data[cur_rel]["fns"].get(name, 0) + int(cnt_str)
            elif line == "end_of_record":
                cur = cur_rel = None
    return data


def _parse_diff_hunks(diff_path: str) -> dict:
    """
    Parse a unified diff into per-file hunk data for line-number remapping.

    Returns dict: rel_path -> list of hunk dicts, each containing:
      old_start, old_count, new_start, new_count,
      removed     - set of old line numbers deleted by this PR,
      context_map - dict mapping old_line -> new_line for unchanged context lines.

    This replaces `lcov --diff` (removed in lcov 2.x) with pure Python.
    """
    file_hunks: dict = {}
    current_file = None
    current_hunk = None
    old_pos = new_pos = 0

    re_file_hdr = re.compile(r"^\+\+\+ b/(.*)$")
    re_hunk_hdr = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

    with open(diff_path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")

            m = re_file_hdr.match(line)
            if m:
                current_file = m.group(1)
                if current_file == "/dev/null":
                    current_file = None
                else:
                    file_hunks.setdefault(current_file, [])
                current_hunk = None
                continue

            m = re_hunk_hdr.match(line)
            if m and current_file is not None:
                old_start = int(m.group(1))
                old_count = int(m.group(2)) if m.group(2) is not None else 1
                new_start = int(m.group(3))
                new_count = int(m.group(4)) if m.group(4) is not None else 1
                current_hunk = {
                    "old_start": old_start,
                    "old_count": old_count,
                    "new_start": new_start,
                    "new_count": new_count,
                    "removed": set(),
                    "context_map": {},
                }
                file_hunks[current_file].append(current_hunk)
                old_pos = old_start
                new_pos = new_start
                continue

            if current_hunk is None or current_file is None:
                continue

            if line.startswith("-"):
                current_hunk["removed"].add(old_pos)
                old_pos += 1
            elif line.startswith("+"):
                new_pos += 1
            elif line.startswith(" "):
                current_hunk["context_map"][old_pos] = new_pos
                old_pos += 1
                new_pos += 1

    return file_hunks


def _remap_line(old_line: int, hunks: list) -> int | None:
    """
    Map a line number from the old (baseline) file to the new (current) file.
    Returns None only for pure deletions with no replacement (new_count == 0).

    For lines that were replaced/commented-out (new_count > 0), returns an
    approximate new line position so that LBC analysis can still report them.
    This is important for the common case of "commenting out tests": the test
    code appears as '-' lines in the diff, so without this approximation the
    coverage loss would be silently ignored.

    Walks hunks in order, accumulating the net line-count delta from each hunk
    that ends before old_line.  For lines inside a hunk, uses context_map
    (unchanged lines) or approximates from the hunk's new_start offset.
    """
    offset = 0
    for h in hunks:
        hunk_old_end = h["old_start"] + h["old_count"] - 1
        if old_line < h["old_start"]:
            break  # line is before this hunk; accumulated offset is final
        if old_line > hunk_old_end:
            offset += h["new_count"] - h["old_count"]
            continue
        # line is inside this hunk
        if old_line in h["context_map"]:
            return h["context_map"][old_line]
        if old_line in h["removed"]:
            if h["new_count"] == 0:
                return None  # pure deletion — code is gone entirely
            # Replaced or commented-out: approximate position within the new hunk.
            # For 1-to-1 replacements (e.g. adding // prefix) this is exact.
            approx = h["new_start"] + (old_line - h["old_start"])
            return min(approx, h["new_start"] + h["new_count"] - 1)
        return old_line + offset  # fallback; should not normally occur
    return old_line + offset


if __name__ == "__main__":
    DIFF = f"{temp_dir}/changes.diff"
    INFO = f"{temp_dir}/current.changed.info"

    # --- parse changed NEW-line ranges from unified diff (0 context)
    ranges = defaultdict(list)  # relpath -> list[(start,end)]
    cur = None
    re_file = re.compile(r"^\+\+\+ b/(.*)$")
    re_hunk = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")

    with open(DIFF, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = re_file.match(line.rstrip("\n"))
            if m:
                p = m.group(1)
                cur = None if p == "/dev/null" else p
                continue
            m = re_hunk.match(line)
            if m and cur:
                start = int(m.group(1))
                ln = int(m.group(2) or "1")
                if ln > 0:
                    ranges[cur].append((start, start + ln - 1))

    if not ranges:
        msg = "No changed-line hunks found (is your diff empty?)."
        print(msg)
        r = Result.create_from(
            name="Print Uncovered Code",
            status=Result.Status.SUCCESS,
            info=msg,
        )
        r.set_comment(msg)
        r.dump()
        sys.exit(0)

    # helper: does absolute SF path correspond to repo-relative path?
    def sf_matches(sf, rel):
        return sf.endswith("/" + rel) or sf.endswith(rel)

    # --- stream parse LCOV .info: only DA lines; count only changed lines
    total = covered = noise_skipped = 0
    uncovered = []  # (relpath, line)

    active_rel = None
    active_ranges = None

    with open(INFO, encoding="utf-8", errors="replace") as f:
        for line in f:
            if line.startswith("SF:"):
                sf = line[3:].strip()
                active_rel = None
                active_ranges = None
                # map SF absolute path -> one of our changed relpaths
                for rel in ranges.keys():
                    if sf_matches(sf, rel):
                        active_rel = rel
                        active_ranges = ranges[rel]
                        break
                continue

            if not active_ranges:
                continue

            if line.startswith("DA:"):
                # DA:<line>,<count>[,<checksum>]
                parts = line[3:].split(",", 2)
                ln = int(parts[0])
                cnt = int(parts[1])

                # check if ln is in any changed range (ranges are small; linear scan OK)
                in_changed = any(a <= ln <= b for a, b in active_ranges)
                if not in_changed:
                    continue

                # Skip lines that are not meant to be executed (error paths, etc.)
                if _is_noise(active_rel, ln):
                    noise_skipped += 1
                    continue

                total += 1
                if cnt > 0:
                    covered += 1
                else:
                    uncovered.append((active_rel, ln))

    CONTEXT = 2  # lines before/after
    MAX_PRINT = 200  # max uncovered lines to print total

    if total == 0:
        msg = "N/A (no coverable changed lines)"
        print(f"PR changed-lines coverage: {msg}")
    else:
        pct = 100.0 * covered / total
        msg = f"{pct:.2f}% ({covered}/{total})"
        print(msg)

    if uncovered:
        print("\nUncovered changed code (with context):\n")

        # group uncovered lines by file
        by_file = defaultdict(list)
        for rel, ln in uncovered[:MAX_PRINT]:
            by_file[rel].append(ln)

        for rel in sorted(by_file.keys()):
            lines = _load_source(rel)  # already cached

            print("=" * 80)
            print(rel)
            print("=" * 80)

            if not lines:
                abs_path = os.path.join(repo_root, rel)
                print(f"  [source file not found: {abs_path}]")
                continue

            # sort + deduplicate
            file_lines = sorted(set(by_file[rel]))

            # merge contiguous lines into blocks
            blocks = []
            start = prev = file_lines[0]

            for ln in file_lines[1:]:
                if ln == prev + 1:
                    prev = ln
                else:
                    blocks.append((start, prev))
                    start = prev = ln
            blocks.append((start, prev))

            # print blocks
            for block_start, block_end in blocks:
                start_line = max(1, block_start - CONTEXT)
                end_line = min(len(lines), block_end + CONTEXT)

                print(f"\n--- uncovered block {block_start}-{block_end} ---")

                for i in range(start_line, end_line + 1):
                    prefix = ">>" if block_start <= i <= block_end else "  "
                    code = lines[i - 1].rstrip("\n")
                    print(f"{prefix} {i:6d} | {code}")
    else:
        print("No uncovered changed lines found.")

    # --- Lost Baseline Coverage (LBC) ---
    # Detect lines/functions that were covered in the master baseline but are no longer
    # covered in the current PR build.  This catches regressions introduced by test
    # modifications that silently drop existing coverage.
    #
    # Line numbers in baseline.changed.info may differ from current.changed.info
    # because the PR itself edited those files.  We remap them through the unified
    # diff in pure Python (lcov --diff was removed in lcov 2.x).
    BASELINE_INFO = f"{temp_dir}/baseline.changed.info"

    lbc_lines: list[tuple[str, int]] = []     # (relpath, lineno)
    lbc_fns: list[tuple[str, str]] = []       # (relpath, fn_name)

    if os.path.exists(BASELINE_INFO) and os.path.getsize(BASELINE_INFO) > 0:
        diff_hunks = _parse_diff_hunks(DIFF)
        base_data = _parse_info(BASELINE_INFO)
        curr_data = _parse_info(INFO)

        _empty: dict = {"lines": {}, "fns": {}}
        for rel in sorted(base_data):
            b = base_data[rel]
            # Use empty coverage when the SF is absent from current entirely
            # (e.g. a test file was commented out and produced zero coverage).
            c = curr_data.get(rel, _empty)
            hunks = diff_hunks.get(rel, [])

            for old_ln, bcnt in b["lines"].items():
                if bcnt == 0:
                    continue
                new_ln = _remap_line(old_ln, hunks)
                if new_ln is None:
                    continue  # line was deleted by this PR — expected
                if c["lines"].get(new_ln, 0) == 0 and not _is_noise(rel, new_ln):
                    lbc_lines.append((rel, new_ln))

            for fn, bcnt in b["fns"].items():
                if bcnt > 0 and c["fns"].get(fn, 0) == 0:
                    lbc_fns.append((rel, fn))

    if lbc_lines:
        print(f"\n=== Lost Baseline Coverage: {len(lbc_lines)} lines ===\n")

        by_file: dict[str, list[int]] = defaultdict(list)
        for rel, ln in lbc_lines[:MAX_PRINT]:
            by_file[rel].append(ln)

        for rel in sorted(by_file.keys()):
            lines = _load_source(rel)

            print("=" * 80)
            print(rel)
            print("=" * 80)

            if not lines:
                abs_path = os.path.join(repo_root, rel)
                print(f"  [source file not found: {abs_path}]")
                continue

            file_lines = sorted(set(by_file[rel]))
            blocks = []
            start = prev = file_lines[0]
            for ln in file_lines[1:]:
                if ln == prev + 1:
                    prev = ln
                else:
                    blocks.append((start, prev))
                    start = prev = ln
            blocks.append((start, prev))

            for block_start, block_end in blocks:
                start_line = max(1, block_start - CONTEXT)
                end_line = min(len(lines), block_end + CONTEXT)
                print(f"\n--- lost coverage block {block_start}-{block_end} ---")
                for i in range(start_line, end_line + 1):
                    prefix = ">>" if block_start <= i <= block_end else "  "
                    code = lines[i - 1].rstrip("\n")
                    print(f"{prefix} {i:6d} | {code}")
    else:
        print("No lost baseline coverage found.")

    if lbc_fns:
        print(f"\n=== Lost Baseline Coverage: {len(lbc_fns)} functions ===\n")
        for rel, fn in lbc_fns[:MAX_PRINT]:
            print(f"  {rel}: {fn}")

    lbc_count = len(lbc_lines)
    lbc_fn_count = len(lbc_fns)

    parts = [msg]
    if lbc_count > 0 or lbc_fn_count > 0:
        lbc_details = []
        if lbc_count > 0:
            lbc_details.append(f"{lbc_count} line(s)")
        if lbc_fn_count > 0:
            lbc_details.append(f"{lbc_fn_count} function(s)")
        parts.append(f"lost baseline coverage: {', '.join(lbc_details)}")
    full_msg = " | ".join(parts)

    r = Result.create_from(
        name="Print Uncovered Code",
        status=Result.Status.SUCCESS,
        info=full_msg,
        with_info_from_results=True,
    )
    r.set_comment(full_msg)
    r.ext["changed_lines_total"] = total
    r.ext["changed_lines_covered"] = covered
    r.ext["changed_lines_cov"] = pct if total > 0 else 0.0
    r.ext["lbc_lines"] = lbc_count
    r.ext["lbc_fns"] = lbc_fn_count
    r.dump()
