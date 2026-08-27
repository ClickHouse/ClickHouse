"""Find code that is newly covered by the tests added/modified in this PR.

For a tests-only PR the global coverage delta is too small to be informative
(a single new test rarely shifts overall coverage by more than 0.01 pp).
What is actually informative is: "which previously-uncovered lines/functions
become covered when the new test(s) run?". This script computes that by diffing
the master baseline `.info` file against the current build's `.info` file and
reporting every (file, line) and (file, function) pair whose execution count
transitioned from zero in the baseline to greater-than-zero in the current run.

A handful of caveats are baked into the output of this script:

  * The "baseline" is the nearest ancestor master commit that has a published
    coverage report (downloaded earlier by generate_diff_coverage_report.sh).
    Coverage drift since that baseline — including unrelated tests added on
    master — will appear here too. For a typical tests-only PR the dominant
    signal is still the PR's own contribution.
  * Files that did not exist in the baseline are skipped entirely. If they
    did not exist, they are unrelated to "tests covering previously-uncovered
    code" — they may simply be new files committed in master since baseline.
  * Lines categorised as `noise` by print_uncovered_code (LOGICAL_ERROR,
    UNREACHABLE, structural braces, ...) are filtered out so the report is
    not dominated by error-path bookkeeping.
"""
import os
import re
import subprocess
import sys
from collections import defaultdict
from ci.praktika.result import Result
from ci.praktika.utils import Utils

repo_root = Utils.cwd()
temp_dir = f"{repo_root}/ci/tmp/"

_NOISE_PATTERNS = [
    re.compile(r"\bLOGICAL_ERROR\b"),
    re.compile(r"\bUNREACHABLE\s*\("),
    re.compile(r"__builtin_unreachable\s*\("),
    re.compile(r"\babort\s*\(\s*\)"),
    re.compile(r"\bstd::terminate\s*\("),
    re.compile(r"\babortOnFailedAssertion\s*\("),
]

_STRUCTURAL_LINES = frozenset({"", "{", "}", "};", "else", "else{", "else {"})

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
    if text in _STRUCTURAL_LINES:
        return True
    return any(p.search(text) for p in _NOISE_PATTERNS)


def _normalize_sf(sf: str) -> str:
    # Strip the machine-specific path prefix so we can join paths from a
    # baseline tracefile (different CI runner) with the current run.
    marker = "/ClickHouse/"
    idx = sf.rfind(marker)
    if idx >= 0:
        return sf[idx + len(marker):]
    return sf


def _parse_info(path: str) -> dict:
    """Parse an LCOV `.info` file into
    `{rel_path: {"lines": {ln: cnt}, "fns": {name: cnt}, "branches": {(ln,blk,br): cnt}}}`.

    Counts accumulate when the same SF appears more than once (which happens
    after lcov merges multiple tracefiles).
    """
    data: dict = {}
    cur_rel: str | None = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if line.startswith("SF:"):
                cur_rel = _normalize_sf(line[3:])
                data.setdefault(cur_rel, {"lines": {}, "fns": {}, "branches": {}})
            elif not cur_rel:
                continue
            elif line.startswith("DA:"):
                parts = line[3:].split(",", 2)
                # lcov may emit counts in scientific notation (e.g. 3.69e+19) when
                # multiple .info files are merged and counters overflow. Use float
                # first to handle that, then convert to int (coverage only cares
                # whether the count is zero or non-zero).
                ln, cnt = int(parts[0]), int(float(parts[1]))
                data[cur_rel]["lines"][ln] = data[cur_rel]["lines"].get(ln, 0) + cnt
            elif line.startswith("BRDA:"):
                # BRDA:<line>,<block>,<branch>,<count|-|0>
                parts = line[5:].split(",", 3)
                if len(parts) == 4:
                    ln_b = int(parts[0])
                    blk, br, cnt_s = parts[1], parts[2], parts[3]
                    bkey = (ln_b, blk, br)
                    prev = data[cur_rel]["branches"].get(bkey, 0)
                    if cnt_s != "-":
                        data[cur_rel]["branches"][bkey] = prev + int(float(cnt_s))
                    elif bkey not in data[cur_rel]["branches"]:
                        data[cur_rel]["branches"][bkey] = 0
            elif line.startswith("FNDA:"):
                cnt_str, name = line[5:].split(",", 1)
                data[cur_rel]["fns"][name] = (
                    data[cur_rel]["fns"].get(name, 0) + int(float(cnt_str))
                )
            elif line == "end_of_record":
                cur_rel = None
    return data


def _format_block_preview(rel: str, lines_in_block: list[int], context: int = 2) -> list[str]:
    """Render newly-covered lines for one file as merged blocks.

    Adjacent newly-covered lines are grouped first; then *near*-adjacent
    groups whose surrounding-context windows would touch or overlap are
    merged into a single block, so the reader sees one cohesive snippet
    instead of several near-identical stanzas that re-print the same
    intermediate code. Inside the merged block, only the actually
    newly-covered lines (the original set) carry the ">>" marker — lines
    that fall in the gap between original sub-blocks are shown without
    highlight as ordinary context.
    """
    out: list[str] = []
    src = _load_source(rel)
    if not src:
        out.append(f"  [source file not found: {os.path.join(repo_root, rel)}]")
        return out
    nc_set = set(lines_in_block)
    if not nc_set:
        return out
    file_lines = sorted(nc_set)

    # Group consecutive newly-covered lines.
    raw_blocks: list[tuple[int, int]] = []
    start = prev = file_lines[0]
    for ln in file_lines[1:]:
        if ln == prev + 1:
            prev = ln
        else:
            raw_blocks.append((start, prev))
            start = prev = ln
    raw_blocks.append((start, prev))

    # Merge groups whose context windows would touch/overlap. With `context`
    # lines on each side, two groups whose original ends are within
    # `2 * context + 1` lines of each other would otherwise have their
    # printed regions duplicate one another (and the gap lines themselves
    # are useful context, so keeping them in a single block is strictly
    # better than splitting).
    merge_gap = 2 * context + 1
    merged: list[tuple[int, int]] = [raw_blocks[0]]
    for s, e in raw_blocks[1:]:
        ps, pe = merged[-1]
        if s - pe <= merge_gap:
            merged[-1] = (ps, e)
        else:
            merged.append((s, e))

    for block_start, block_end in merged:
        s = max(1, block_start - context)
        e = min(len(src), block_end + context)
        out.append(f"\n--- newly covered block {block_start}-{block_end} ---")
        for i in range(s, e + 1):
            pfx = ">>" if i in nc_set else "  "
            out.append(f"{pfx} {i:6d} | {src[i - 1].rstrip()}")
    return out


def _parse_extra_diff_hunks(diff_text: str) -> tuple[dict[str, list], set[str]]:
    """Parse a unified git diff (extra_sha -> primary_sha) into per-file hunk structures.

    Returns (file_hunks, deleted_files):
      file_hunks:    {new_path: [hunk, ...]} for files that survived (possibly modified)
      deleted_files: set of OLD paths for files deleted or renamed away between the two
                     commits — these must not be remapped to identity; instead skip them
                     entirely so their obsolete counters do not leak into stable totals.

    Each hunk contains:
      old_start, old_count, new_start, new_count
      removed:     set of old line numbers deleted going from extra to primary
      context_map: {old_line: new_line} for lines that survived unchanged
    """
    file_hunks: dict[str, list] = {}
    deleted_files: set[str] = set()
    current_old: str | None = None   # --- a/... path
    current_file: str | None = None  # +++ b/... path (None = /dev/null)
    current_hunk = None
    old_pos = new_pos = 0

    re_old_hdr = re.compile(r"^--- (?:a/)?(.*)$")
    re_new_hdr = re.compile(r"^\+\+\+ (?:b/)?(.*)$")
    re_hunk_hdr = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

    for line in diff_text.splitlines():
        m = re_old_hdr.match(line)
        if m:
            p = m.group(1)
            current_old = None if p == "/dev/null" else p
            current_hunk = None
            continue

        m = re_new_hdr.match(line)
        if m:
            p = m.group(1)
            current_file = None if p == "/dev/null" else p
            if current_file:
                file_hunks.setdefault(current_file, [])
            elif current_old:
                # +++ /dev/null means this old path was deleted or renamed away.
                # Record it so the accumulator can skip it instead of identity-mapping.
                deleted_files.add(current_old)
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

    return file_hunks, deleted_files


def _remap_old_to_new(old_line: int, hunks: list) -> int | None:
    """Map an old (extra baseline) line number to the primary baseline's coordinate system.

    Returns None if the line was deleted between the two commits (not in primary).
    Lines outside all hunks are shifted by the accumulated offset of preceding hunks.
    """
    offset = 0
    for h in hunks:
        hunk_old_end = h["old_start"] + h["old_count"] - 1
        if old_line < h["old_start"]:
            break
        if old_line > hunk_old_end:
            offset += h["new_count"] - h["old_count"]
            continue
        # Line is inside this hunk
        if old_line in h["context_map"]:
            return h["context_map"][old_line]
        if old_line in h["removed"]:
            return None  # deleted in primary
        return old_line + offset  # fallback
    return old_line + offset


if __name__ == "__main__":
    BASE = os.environ.get("COVERAGE_BASE_INFO", f"{temp_dir}/base_llvm_coverage.info")
    # For partial runs (e.g. only stateless tests changed), llvm_coverage_job.py
    # creates merged_llvm_coverage.info = union(pr.info, master.info) and passes it
    # via COVERAGE_CURR_INFO so that gained lines are computed against the full
    # coverage picture, not just the partial run.
    CURR = os.environ.get("COVERAGE_CURR_INFO", f"{temp_dir}/llvm_coverage.info")
    # Up to 4 extra baselines downloaded by generate_diff_coverage_report.sh.
    # We intersect their zero-sets: a line must be uncovered in ALL of them to
    # pass the filter. More baselines = fewer false positives from run-to-run
    # flicker. The list is intentionally open-ended so raising TARGET_EXTRA_BASELINES
    # in the shell script automatically takes effect here.
    EXTRA_BASELINE_PATHS = [
        f"{temp_dir}/base_llvm_coverage_{i}.info" for i in range(2, 8)
    ]

    if not (os.path.exists(BASE) and os.path.exists(CURR)):
        msg = "Baseline or current .info file missing - skipping newly-covered analysis."
        print(msg)
        r = Result.create_from(name="Newly Covered Code", status=Result.Status.OK, info=msg)
        r.set_comment("")
        r.dump()
        sys.exit(0)

    print(f"Parsing baseline coverage from {BASE} ...")
    base_data = _parse_info(BASE)
    print(f"  {len(base_data)} files in baseline")
    print(f"Parsing current coverage from {CURR} ...")
    curr_data = _parse_info(CURR)
    print(f"  {len(curr_data)} files in current")

    # Two stable union sets for computing the global coverage delta:
    #
    #   _stable_base = Union(m1, m2, ..., mN)        — all master runs
    #   _stable_pr   = Union(m1, m2, ..., mN, PR)    — master runs plus PR
    #
    # Delta = _stable_pr − _stable_base ≥ 0 always (union is monotone).
    # The delta shows only lines the PR adds that no master run covered.
    # LBC (lines lost vs master) is intentionally omitted — with this
    # definition it is always empty by construction.
    #
    # We avoid `lcov -a` which corrupts FNDA records on some lcov 2.1+ versions.
    _stable_base_line_cov: set[tuple[str, int]] = set()
    _stable_base_line_tot: set[tuple[str, int]] = set()
    _stable_base_fn_cov: set[tuple[str, str]] = set()
    _stable_base_fn_tot: set[tuple[str, str]] = set()
    _stable_base_br_cov: set[tuple[str, int, str, str]] = set()
    _stable_base_br_tot: set[tuple[str, int, str, str]] = set()
    # PR side: starts with the primary baseline (m1); ALL extras added in
    # the loop below; PR (curr_data) added after the loop.
    _stable_pr_line_cov: set[tuple[str, int]] = set()
    _stable_pr_line_tot: set[tuple[str, int]] = set()
    _stable_pr_fn_cov: set[tuple[str, str]] = set()
    _stable_pr_fn_tot: set[tuple[str, str]] = set()
    _stable_pr_br_cov: set[tuple[str, int, str, str]] = set()
    _stable_pr_br_tot: set[tuple[str, int, str, str]] = set()

    def _accum(data: dict,
               lc: set, lt: set, fc: set, ft: set,
               bc: set, bt: set) -> None:
        """Accumulate without remapping (for primary baseline and PR run)."""
        for _rel, _v in data.items():
            for _ln, _cnt in _v["lines"].items():
                lt.add((_rel, _ln))
                if _cnt > 0:
                    lc.add((_rel, _ln))
            for _fn, _cnt in _v["fns"].items():
                ft.add((_rel, _fn))
                if _cnt > 0:
                    fc.add((_rel, _fn))
            for (_ln, _blk, _br), _cnt in _v.get("branches", {}).items():
                bkey = (_rel, _ln, _blk, _br)
                bt.add(bkey)
                if _cnt > 0:
                    bc.add(bkey)

    def _accum_with_remap(data: dict, file_hunks: dict, deleted_files: set,
                          lc: set, lt: set, fc: set, ft: set,
                          bc: set, bt: set) -> None:
        """Accumulate with line-number remapping for extra baselines.

        Lines from each extra baseline were produced from a different master
        commit whose source may have shifted relative to the primary. The
        file_hunks mapping (from git diff <extra_sha>..<primary_sha>) translates
        each line to its position in the primary baseline's coordinate system so
        all stable sets share a single consistent coordinate space.
        Lines deleted between the extra and primary are dropped (new_ln = None).
        Functions are identified by mangled name — stable across minor source
        changes — and need no remapping.
        Branch records carry a line number and are remapped the same way as lines.
        Files in deleted_files were deleted or renamed away before primary; they
        are skipped entirely rather than identity-mapped, which would leak obsolete
        counters into stable totals.
        """
        for _rel, _v in data.items():
            if _rel in deleted_files:
                continue  # file was deleted/renamed — skip to avoid ghost counters
            hunks = file_hunks.get(_rel, [])
            for _ln, _cnt in _v["lines"].items():
                if hunks:
                    new_ln = _remap_old_to_new(_ln, hunks)
                    if new_ln is None:
                        continue  # line deleted between extra and primary
                    key = (_rel, new_ln)
                else:
                    key = (_rel, _ln)  # file unchanged — identity mapping
                lt.add(key)
                if _cnt > 0:
                    lc.add(key)
            for _fn, _cnt in _v["fns"].items():
                ft.add((_rel, _fn))
                if _cnt > 0:
                    fc.add((_rel, _fn))
            for (_ln, _blk, _br), _cnt in _v.get("branches", {}).items():
                if hunks:
                    new_ln = _remap_old_to_new(_ln, hunks)
                    if new_ln is None:
                        continue
                    bkey = (_rel, new_ln, _blk, _br)
                else:
                    bkey = (_rel, _ln, _blk, _br)
                bt.add(bkey)
                if _cnt > 0:
                    bc.add(bkey)

    # Primary (m1) goes into both sides.
    _accum(base_data,
           _stable_base_line_cov, _stable_base_line_tot,
           _stable_base_fn_cov,  _stable_base_fn_tot,
           _stable_base_br_cov,  _stable_base_br_tot)
    _accum(base_data,
           _stable_pr_line_cov, _stable_pr_line_tot,
           _stable_pr_fn_cov,   _stable_pr_fn_tot,
           _stable_pr_br_cov,   _stable_pr_br_tot)

    # Stream each extra baseline: parse, accumulate into both stable sets, discard.
    # Since stable_pr = Union(m1..mN, PR), every extra goes into both stable_base
    # and stable_pr with the same remapping. There is no need to retain parsed dicts
    # across iterations — doing so would hold ~530 MB × N_extras in memory at once.
    extra_baselines_used = 0

    primary_sha_path = f"{temp_dir}/base_llvm_coverage.sha"
    primary_sha = open(primary_sha_path).read().strip() if os.path.exists(primary_sha_path) else ""

    for extra_path in EXTRA_BASELINE_PATHS:
        if not (os.path.exists(extra_path) and os.path.getsize(extra_path) > 0):
            continue

        extra_sha_path = extra_path.replace(".info", ".sha")
        extra_sha = open(extra_sha_path).read().strip() if os.path.exists(extra_sha_path) else ""

        # Compute per-file line-number remapping from extra_sha -> primary_sha.
        file_hunks: dict[str, list] = {}
        deleted_files: set[str] = set()
        changed_file_count = 0
        if primary_sha and extra_sha and primary_sha != extra_sha:
            try:
                diff_result = subprocess.run(
                    ["git", "-C", repo_root, "diff", "--unified=0",
                     f"{extra_sha}..{primary_sha}"],
                    capture_output=True, text=True, timeout=120,
                )
                if diff_result.returncode == 0 and diff_result.stdout:
                    file_hunks, deleted_files = _parse_extra_diff_hunks(diff_result.stdout)
                    changed_file_count = len(file_hunks)
            except Exception as _e:
                print(f"  Warning: git diff failed for {os.path.basename(extra_path)}: {_e}")

        print(
            f"Parsing extra baseline coverage from {extra_path} "
            f"(sha={extra_sha[:12] if extra_sha else 'unknown'}, "
            f"{changed_file_count} files remapped, {len(deleted_files)} deleted/renamed skipped) ..."
        )
        _bx = _parse_info(extra_path)
        print(f"  {len(_bx)} files in {os.path.basename(extra_path)}")

        # Accumulate into both stable sets immediately, then drop the parsed dict.
        # stable_pr = Union(m1..mN, PR) so every extra feeds both sides identically.
        _accum_with_remap(_bx, file_hunks, deleted_files,
                          _stable_base_line_cov, _stable_base_line_tot,
                          _stable_base_fn_cov,   _stable_base_fn_tot,
                          _stable_base_br_cov,   _stable_base_br_tot)
        _accum_with_remap(_bx, file_hunks, deleted_files,
                          _stable_pr_line_cov, _stable_pr_line_tot,
                          _stable_pr_fn_cov,   _stable_pr_fn_tot,
                          _stable_pr_br_cov,   _stable_pr_br_tot)
        del _bx
        extra_baselines_used += 1

    # PR side: no remapping needed (same source/binary epoch as m1).
    _accum(curr_data,
           _stable_pr_line_cov, _stable_pr_line_tot,
           _stable_pr_fn_cov,   _stable_pr_fn_tot,
           _stable_pr_br_cov,   _stable_pr_br_tot)

    have_extra = extra_baselines_used > 0
    if have_extra:
        total_baselines = 1 + extra_baselines_used
        print(
            f"  cross-validation enabled across {total_baselines} master baselines "
            f"(line numbers remapped to primary coordinate system via git diff): "
            f"{len(_stable_base_line_cov):,} lines covered in at least one master run"
        )
    else:
        print(
            "Note: no extra master baselines available - falling back to "
            "single-baseline mode. The count below may include run-to-run "
            "coverage variance (~1000 lines is typical noise between two "
            "adjacent master runs)."
        )

    # Overall coverage delta: stable baseline (Python union of all baselines)
    # vs current (PR run). Using the Python union avoids `lcov -a` which
    # corrupts FNDA records on lcov 2.1+, producing wrong function percentages.
    def _totals(data: dict) -> tuple[int, int, int, int]:
        l_tot = l_hit = f_tot = f_hit = 0
        for v in data.values():
            for cnt in v["lines"].values():
                l_tot += 1
                if cnt > 0:
                    l_hit += 1
            for cnt in v["fns"].values():
                f_tot += 1
                if cnt > 0:
                    f_hit += 1
        return l_tot, l_hit, f_tot, f_hit

    # Delta = stable_pr − stable_base = Union(m1..mN-1, PR) − Union(m1..mN).
    # Lines in m1..mN-1 cancel from both sides; only PR vs mN contributes.
    # Use the union of both total-sets as the shared denominator so percentages
    # are comparable.
    _all_tot    = _stable_base_line_tot | _stable_pr_line_tot
    _all_fn_tot = _stable_base_fn_tot   | _stable_pr_fn_tot
    _all_br_tot = _stable_base_br_tot   | _stable_pr_br_tot
    b_lt = c_lt = len(_all_tot)       # shared line denominator
    b_ft = c_ft = len(_all_fn_tot)    # shared function denominator
    b_bt = c_bt = len(_all_br_tot)    # shared branch denominator
    b_lh = len(_stable_base_line_cov)
    b_fh = len(_stable_base_fn_cov)
    b_bh = len(_stable_base_br_cov)
    c_lh = len(_stable_pr_line_cov)
    c_fh = len(_stable_pr_fn_cov)
    c_bh = len(_stable_pr_br_cov)
    # Fall back to primary-only counts if no extras (single-baseline mode).
    if not _all_tot:
        b_lt = c_lt = len(_stable_base_line_tot) or 1
        b_ft = c_ft = len(_stable_base_fn_tot) or 1
        b_bt = c_bt = len(_stable_base_br_tot) or 1
    b_lp = (100.0 * b_lh / b_lt) if b_lt else 0.0
    c_lp = (100.0 * c_lh / c_lt) if c_lt else 0.0
    b_fp = (100.0 * b_fh / b_ft) if b_ft else 0.0
    c_fp = (100.0 * c_fh / c_ft) if c_ft else 0.0
    b_bp = (100.0 * b_bh / b_bt) if b_bt else 0.0
    c_bp = (100.0 * c_bh / c_bt) if c_bt else 0.0

    print("\nOverall coverage delta (current vs master baseline):")
    print(
        f"  Lines    : {c_lp - b_lp:+.4f} pp  "
        f"({b_lp:.4f}% -> {c_lp:.4f}%, {c_lh - b_lh:+,} covered)"
    )
    print(
        f"  Functions: {c_fp - b_fp:+.4f} pp  "
        f"({b_fp:.4f}% -> {c_fp:.4f}%, {c_fh - b_fh:+,} covered)"
    )
    print(
        f"  Branches : {c_bp - b_bp:+.4f} pp  "
        f"({b_bp:.4f}% -> {c_bp:.4f}%, {c_bh - b_bh:+,} covered)"
    )

    nc_lines: dict[str, list[int]] = defaultdict(list)
    nc_fns: dict[str, list[str]] = defaultdict(list)

    for rel, c in curr_data.items():
        b = base_data.get(rel)
        if b is None:
            # File did not exist in baseline; transition cannot be attributed
            # to "test newly covers code that existed before". Skip.
            continue
        for ln, cnt in c["lines"].items():
            if cnt <= 0:
                continue
            # Newly covered = PR hits it AND it was cold in every master run
            # (m1..mN, all in primary coordinate system after remapping).
            # _stable_base_line_cov is the union of covered lines across all
            # master baselines, so "not in" means cold in all of them.
            if (rel, ln) in _stable_base_line_cov:
                continue
            if _is_noise(rel, ln):
                continue
            nc_lines[rel].append(ln)
        for fn, cnt in c["fns"].items():
            if cnt <= 0:
                continue
            if (rel, fn) in _stable_base_fn_cov:
                continue
            nc_fns[rel].append(fn)

    total_lines = sum(len(v) for v in nc_lines.values())
    total_fns = sum(len(v) for v in nc_fns.values())
    all_files = set(nc_lines.keys()) | set(nc_fns.keys())
    total_files = len(all_files)

    if total_lines == 0 and total_fns == 0:
        summary = "no newly covered lines or functions"
        # The hook checks `if newly_covered_info:` to decide whether to render
        # the "Newly covered by added/modified tests:" line. For PRs with no
        # signal we want that line omitted entirely (otherwise every CI-fix /
        # test-tweak PR with no real coverage delta would carry a confusing
        # "Newly covered: no newly covered lines or functions" footer), so
        # set the GH-comment payload to empty here while keeping the
        # descriptive summary in the log + Result.info for the artifact.
        comment_text = ""
    else:
        bits: list[str] = []
        if total_lines > 0:
            bits.append(f"{total_lines} line(s)")
        if total_fns > 0:
            bits.append(f"{total_fns} function(s)")
        summary = f"{', '.join(bits)} across {total_files} file(s)"
        comment_text = summary
    print(f"\nNewly covered: {summary}\n")

    file_stats = sorted(
        [
            (rel, len(nc_lines.get(rel, [])), len(nc_fns.get(rel, [])))
            for rel in all_files
        ],
        key=lambda x: (-x[1], -x[2], x[0]),
    )

    # Full file inventory — every file with newly-covered code, ranked by line
    # count. This is the authoritative list; the snippet section below may be
    # capped, but this is not.
    if file_stats:
        print(f"All {len(file_stats)} file(s) with newly-covered code, ranked by line count:")
        for rel, lc, fc in file_stats:
            parts = []
            if lc > 0:
                parts.append(f"{lc} line(s)")
            if fc > 0:
                parts.append(f"{fc} function(s)")
            print(f"  {rel}: {', '.join(parts)}")
        print()

    # Detailed per-file snippet preview. Capped to keep the CI job log readable;
    # the file inventory above always lists every contributor in full, so a
    # truncation here does not lose data — only the inline context.
    MAX_BLOCK_LINES = 2000
    printed = 0
    truncated = False
    if total_lines > 0:
        print("Newly covered code (with context):\n")
        for rel, lc, _ in file_stats:
            if lc == 0:
                continue
            if printed >= MAX_BLOCK_LINES:
                truncated = True
                break
            print("=" * 80)
            print(rel)
            print("=" * 80)
            for out_line in _format_block_preview(rel, nc_lines[rel]):
                print(out_line)
                printed += 1
                if printed >= MAX_BLOCK_LINES:
                    truncated = True
                    break
        if truncated:
            print(
                f"\n[snippet preview capped at {MAX_BLOCK_LINES} lines — "
                f"see the file inventory above for the complete list of "
                f"files with newly-covered code; open those files at the "
                f"listed line numbers for the full context]"
            )

    r = Result.create_from(
        name="Newly Covered Code",
        status=Result.Status.OK,
        info=summary,
    )
    r.set_comment(comment_text)
    r.ext["newly_covered_lines"] = total_lines
    r.ext["newly_covered_fns"] = total_fns
    r.ext["newly_covered_files"] = total_files
    # Stable-union coverage delta for the GitHub comment table.
    r.ext["stable_b_line_cov"]   = b_lp
    r.ext["stable_c_line_cov"]   = c_lp
    r.ext["stable_b_func_cov"]   = b_fp
    r.ext["stable_c_func_cov"]   = c_fp
    r.ext["stable_b_branch_cov"] = b_bp
    r.ext["stable_c_branch_cov"] = c_bp
    r.ext["stable_b_line_hit"]   = b_lh
    r.ext["stable_b_line_tot"]   = b_lt
    r.ext["stable_c_line_hit"]   = c_lh
    r.ext["stable_c_line_tot"]   = c_lt
    r.ext["stable_b_func_hit"]   = b_fh
    r.ext["stable_b_func_tot"]   = b_ft
    r.ext["stable_c_func_hit"]   = c_fh
    r.ext["stable_c_func_tot"]   = c_ft
    r.ext["stable_b_branch_hit"] = b_bh
    r.ext["stable_b_branch_tot"] = b_bt
    r.ext["stable_c_branch_hit"] = c_bh
    r.ext["stable_c_branch_tot"] = c_bt
    # Snapshot the top files into the result for inline rendering in the GH comment.
    r.ext["newly_covered_top_files"] = [
        {"rel": rel, "lines": lc, "fns": fc}
        for rel, lc, fc in file_stats[:5]
    ]
    r.dump()
