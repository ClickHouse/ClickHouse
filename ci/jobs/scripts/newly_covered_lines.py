"""Compare two lcov tracefiles line by line and report coverage transitions.

Used by the `LLVM Coverage` job for PRs that change no coverable C/C++ source
file (a tests-only PR forced with the `ci-coverage` label, or a CI-scripts-only
PR that touches the coverage pipeline). The compiled binary is then identical
to the master baseline, so the per-changed-line differential report has no
input - but the global comparison is exactly what such a PR wants to see:
which lines had zero hits on master and are executed now (what the new tests
add), and which lines lost coverage (baseline noise or removed/disabled tests).

Only `SF:`/`DA:` records are consulted. A line is compared only when both
tracefiles instrument it; with an identical binary the instrumented line sets
match, so lines present on one side only are reported as a diagnostic count
rather than as coverage transitions.
"""

from pathlib import Path

# SF paths embed the runner's workspace (/home/<user>/actions-runner/_work/
# ClickHouse/ClickHouse/src/...), which differs between the runs that produced
# the two tracefiles. Everything after the repo checkout directory is stable.
_REPO_MARKER = "/ClickHouse/ClickHouse/"

# Caps keeping the text report readable; totals are always exact.
MAX_FILES_LISTED = 100
MAX_RANGES_PER_FILE = 30


def normalize_source_path(path: str) -> str:
    pos = path.rfind(_REPO_MARKER)
    if pos != -1:
        return path[pos + len(_REPO_MARKER):]
    return path


def parse_tracefile(path: str) -> dict[str, dict[int, int]]:
    """Return {normalized source file: {line: hit count}}.

    lcov merge output holds one block per file, but blocks for the same file
    (e.g. after path normalization) are folded by summing counts - the same
    semantics lcov itself uses when merging tracefiles.
    """
    coverage: dict[str, dict[int, int]] = {}
    current: dict[int, int] | None = None
    with open(path, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()
            if line.startswith("SF:"):
                current = coverage.setdefault(normalize_source_path(line[3:]), {})
            elif line.startswith("DA:") and current is not None:
                # DA:<line>,<count>[,<checksum>]
                fields = line[3:].split(",")
                lineno = int(fields[0])
                # llvm-cov may emit huge counts in scientific notation.
                count = int(float(fields[1]))
                current[lineno] = current.get(lineno, 0) + count
            elif line == "end_of_record":
                current = None
    return coverage


def compress_line_ranges(lines: list[int]) -> str:
    """[1, 2, 3, 7, 9, 10] -> "1-3, 7, 9-10", capped at MAX_RANGES_PER_FILE."""
    ranges: list[str] = []
    start = prev = lines[0]
    for lineno in lines[1:]:
        if lineno == prev + 1:
            prev = lineno
            continue
        ranges.append(str(start) if start == prev else f"{start}-{prev}")
        start = prev = lineno
    ranges.append(str(start) if start == prev else f"{start}-{prev}")
    if len(ranges) > MAX_RANGES_PER_FILE:
        ranges = ranges[:MAX_RANGES_PER_FILE] + ["..."]
    return ", ".join(ranges)


def _render_transitions(title: str, per_file: dict[str, list[int]], out: list[str]) -> None:
    total = sum(len(v) for v in per_file.values())
    out.append(f"{title}: {total} lines in {len(per_file)} files")
    ordered = sorted(per_file.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    for source_file, lines in ordered[:MAX_FILES_LISTED]:
        out.append(f"  {len(lines):<7} {source_file}: {compress_line_ranges(sorted(lines))}")
    if len(ordered) > MAX_FILES_LISTED:
        out.append(f"  ... and {len(ordered) - MAX_FILES_LISTED} more files")
    out.append("")


def generate_report(current_info: str, baseline_info: str, output_path: str) -> dict:
    """Write the transitions report to output_path and return the totals.

    Returns {"newly_covered": N, "newly_covered_files": F,
             "lost_coverage": M, "lost_coverage_files": G,
             "one_sided_lines": K}.
    """
    baseline = parse_tracefile(baseline_info)
    current = parse_tracefile(current_info)

    newly_covered: dict[str, list[int]] = {}
    lost_coverage: dict[str, list[int]] = {}
    one_sided_lines = 0

    for source_file, current_lines in current.items():
        baseline_lines = baseline.get(source_file)
        if baseline_lines is None:
            one_sided_lines += len(current_lines)
            continue
        for lineno, count in current_lines.items():
            base_count = baseline_lines.get(lineno)
            if base_count is None:
                one_sided_lines += 1
            elif base_count == 0 and count > 0:
                newly_covered.setdefault(source_file, []).append(lineno)
            elif base_count > 0 and count == 0:
                lost_coverage.setdefault(source_file, []).append(lineno)

    for source_file, baseline_lines in baseline.items():
        current_lines = current.get(source_file)
        if current_lines is None:
            one_sided_lines += len(baseline_lines)
        else:
            one_sided_lines += len(baseline_lines.keys() - current_lines.keys())

    out: list[str] = [
        "Line coverage transitions against the master baseline",
        f"Baseline tracefile: {baseline_info}",
        f"Current tracefile : {current_info}",
        "",
    ]
    _render_transitions("Newly covered (0 hits on master, >0 hits now)", newly_covered, out)
    _render_transitions("Lost coverage (>0 hits on master, 0 hits now)", lost_coverage, out)
    if one_sided_lines:
        out.append(
            f"Diagnostic: {one_sided_lines} instrumented lines are present in only "
            "one tracefile. With an identical binary this indicates path "
            "normalization or instrumentation drift; these lines are not counted "
            "as transitions."
        )
    Path(output_path).write_text("\n".join(out) + "\n", encoding="utf-8")

    return {
        "newly_covered": sum(len(v) for v in newly_covered.values()),
        "newly_covered_files": len(newly_covered),
        "lost_coverage": sum(len(v) for v in lost_coverage.values()),
        "lost_coverage_files": len(lost_coverage),
        "one_sided_lines": one_sided_lines,
    }
