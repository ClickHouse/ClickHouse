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
    """Parse an LCOV `.info` file into `{rel_path: {"lines": {ln: cnt}, "fns": {name: cnt}}}`.

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
                data.setdefault(cur_rel, {"lines": {}, "fns": {}})
            elif not cur_rel:
                continue
            elif line.startswith("DA:"):
                parts = line[3:].split(",", 2)
                ln, cnt = int(parts[0]), int(parts[1])
                data[cur_rel]["lines"][ln] = data[cur_rel]["lines"].get(ln, 0) + cnt
            elif line.startswith("FNDA:"):
                cnt_str, name = line[5:].split(",", 1)
                data[cur_rel]["fns"][name] = (
                    data[cur_rel]["fns"].get(name, 0) + int(cnt_str)
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


if __name__ == "__main__":
    BASE = f"{temp_dir}/base_llvm_coverage.info"
    BASE2 = f"{temp_dir}/base_llvm_coverage_2.info"
    CURR = f"{temp_dir}/llvm_coverage.info"

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

    # Cross-validation against a second, older master baseline. Without it,
    # the coverage build's run-to-run variance (typically ~1000 lines flicker
    # between two adjacent master runs) shows up as false-positive newly-
    # covered lines. With it, we keep only lines that are uncovered in BOTH
    # masters — i.e. genuinely cold code, not "sometimes-cold" code that
    # happens to be hit this run by chance. Store baseline-2's zero set as
    # a (rel, key) pair-set rather than a full dict to keep peak memory low.
    have_base2 = os.path.exists(BASE2) and os.path.getsize(BASE2) > 0
    base2_zero_lines: set[tuple[str, int]] = set()
    base2_zero_fns: set[tuple[str, str]] = set()
    if have_base2:
        print(f"Parsing second baseline coverage from {BASE2} ...")
        _b2 = _parse_info(BASE2)
        print(f"  {len(_b2)} files in second baseline")
        for _rel, _v in _b2.items():
            for _ln, _cnt in _v["lines"].items():
                if _cnt == 0:
                    base2_zero_lines.add((_rel, _ln))
            for _fn, _cnt in _v["fns"].items():
                if _cnt == 0:
                    base2_zero_fns.add((_rel, _fn))
        del _b2
        print(
            f"  cross-validation enabled: requiring uncovered in both master baselines "
            f"({len(base2_zero_lines):,} candidate lines / {len(base2_zero_fns):,} functions in B2)"
        )
    else:
        print(
            "Note: second master baseline not available — falling back to "
            "single-baseline mode. The count below may include run-to-run "
            "coverage variance (~1000 lines is typical noise between two "
            "adjacent master runs)."
        )

    # Overall coverage delta vs master baseline, computed from the parsed
    # tracefiles directly. These numbers may diverge from `lcov --summary` by a
    # rounding-level amount (lcov does some extra normalisation), but the trend
    # — improved, unchanged, or regressed — is what tests typically move by a
    # fraction of a percentage point, so 4 decimal places below.
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

    b_lt, b_lh, b_ft, b_fh = _totals(base_data)
    c_lt, c_lh, c_ft, c_fh = _totals(curr_data)
    b_lp = (100.0 * b_lh / b_lt) if b_lt else 0.0
    c_lp = (100.0 * c_lh / c_lt) if c_lt else 0.0
    b_fp = (100.0 * b_fh / b_ft) if b_ft else 0.0
    c_fp = (100.0 * c_fh / c_ft) if c_ft else 0.0

    print("\nOverall coverage delta (current vs master baseline):")
    print(
        f"  Lines    : {c_lp - b_lp:+.4f} pp  "
        f"({b_lp:.4f}% -> {c_lp:.4f}%, {c_lh - b_lh:+,} covered)"
    )
    print(
        f"  Functions: {c_fp - b_fp:+.4f} pp  "
        f"({b_fp:.4f}% -> {c_fp:.4f}%, {c_fh - b_fh:+,} covered)"
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
            if cnt <= 0 or b["lines"].get(ln) != 0:
                continue
            # If a second master baseline was downloaded, require that the line
            # is also uncovered there. If the line is absent or non-zero in B2,
            # treat it as flutter and drop it.
            if have_base2 and (rel, ln) not in base2_zero_lines:
                continue
            if _is_noise(rel, ln):
                continue
            nc_lines[rel].append(ln)
        for fn, cnt in c["fns"].items():
            if cnt <= 0 or b["fns"].get(fn) != 0:
                continue
            if have_base2 and (rel, fn) not in base2_zero_fns:
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

    # The per-function listing used to print mangled C++ symbol names verbatim,
    # which were unreadable (e.g. `_ZNK2DB12_GLOBAL__N_1...`) and added no
    # signal beyond the per-file function counts already shown in the inventory
    # above. Removed.

    r = Result.create_from(
        name="Newly Covered Code",
        status=Result.Status.OK,
        info=summary,
    )
    r.set_comment(comment_text)
    r.ext["newly_covered_lines"] = total_lines
    r.ext["newly_covered_fns"] = total_fns
    r.ext["newly_covered_files"] = total_files
    # Snapshot the top files into the result for inline rendering in the GH comment.
    r.ext["newly_covered_top_files"] = [
        {"rel": rel, "lines": lc, "fns": fc}
        for rel, lc, fc in file_stats[:5]
    ]
    r.dump()
