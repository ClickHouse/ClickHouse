#!/usr/bin/env python3
"""
Deduplicate C++ template instantiations in LLVM-generated LCOV coverage data.

LLVM coverage counts each template instantiation as a separate function, inflating
FNF (functions found) while line coverage stays high because source lines are shared.
For example, `serialize<UInt8>` and `serialize<Float64>` are two functions pointing
at the same source line — if either is called, the template is exercised.

Algorithm: within each SF section, group FN: records by line number. If any
instantiation in the group has a non-zero FNDA: count, propagate the max count to
all uncovered instantiations in the group. Recompute FNH: and FNF: accordingly.

Usage:
    python3 dedup_lcov_instantiations.py input.info [output.info]

If output.info is omitted, modifies input.info in-place (via a temp file).
"""

import re
import sys
import os
import tempfile
from collections import defaultdict


def process_section(lines: list[str]) -> list[str]:
    """
    Process one SF...end_of_record section.

    Returns a list of output lines (each ending with '\\n').
    """
    # --- First pass: collect FN and FNDA records ---
    # fn_records: func_name -> source line_no (first seen wins)
    fn_records: dict[str, int] = {}
    fn_order: list[str] = []       # preserve declaration order for output
    fnda_map: dict[str, int] = {}  # func_name -> call count

    for line in lines:
        if line.startswith("FN:"):
            m = re.match(r"^FN:(\d+),(.*)", line)
            if m:
                line_no, func_name = int(m.group(1)), m.group(2)
                if func_name not in fn_records:
                    fn_records[func_name] = line_no
                    fn_order.append(func_name)
        elif line.startswith("FNDA:"):
            m = re.match(r"^FNDA:(\d+),(.*)", line)
            if m:
                fnda_map[m.group(2)] = int(m.group(1))

    if not fn_records:
        # No function records — return as-is
        return [l if l.endswith("\n") else l + "\n" for l in lines]

    # --- Dedup: group by source line, propagate max count ---
    line_to_funcs: dict[int, list[str]] = defaultdict(list)
    for func_name, line_no in fn_records.items():
        line_to_funcs[line_no].append(func_name)

    new_fnda = dict(fnda_map)
    for funcs in line_to_funcs.values():
        max_count = max(new_fnda.get(f, 0) for f in funcs)
        if max_count > 0:
            for f in funcs:
                if new_fnda.get(f, 0) == 0:
                    new_fnda[f] = max_count

    # --- Recompute FNH / FNF ---
    fnf = len(fn_records)
    fnh = sum(1 for f in fn_records if new_fnda.get(f, 0) > 0)

    # Functions that gained coverage but had no original FNDA: line
    needs_new_fnda = {
        f for f in fn_records if f not in fnda_map and new_fnda.get(f, 0) > 0
    }

    # --- Second pass: rebuild section ---
    result: list[str] = []
    emitted_new_fnda = False

    for line in lines:
        if line == "end_of_record":
            # Flush any newly-covered functions before closing the record
            if not emitted_new_fnda and needs_new_fnda:
                for f in sorted(needs_new_fnda):
                    result.append(f"FNDA:{new_fnda[f]},{f}\n")
                emitted_new_fnda = True
            result.append("end_of_record\n")
        elif line.startswith("FNDA:"):
            m = re.match(r"^FNDA:(\d+),(.*)", line)
            if m:
                func_name = m.group(2)
                result.append(f"FNDA:{new_fnda.get(func_name, 0)},{func_name}\n")
            else:
                result.append(line if line.endswith("\n") else line + "\n")
            # After the last original FNDA block, insert new ones
            # (we check on next non-FNDA line below)
        elif line.startswith("FNH:"):
            result.append(f"FNH:{fnh}\n")
        elif line.startswith("FNF:"):
            # Flush newly-covered FNDA lines before FNF (natural insertion point)
            if not emitted_new_fnda and needs_new_fnda:
                for f in sorted(needs_new_fnda):
                    result.append(f"FNDA:{new_fnda[f]},{f}\n")
                emitted_new_fnda = True
            result.append(f"FNF:{fnf}\n")
        else:
            result.append(line if line.endswith("\n") else line + "\n")

    return result


def process_info_file(in_path: str, out_path: str) -> tuple[int, int, int, int, int]:
    """
    Process an LCOV .info file, deduplicating template instantiations.

    Returns (total_sections, total_fn_before, total_fn_after) for reporting.
    """
    total_sections = 0
    total_fn_found_before = 0
    total_fn_hit_before = 0
    total_fn_found_after = 0
    total_fn_hit_after = 0

    with open(in_path, "r", encoding="utf-8", errors="replace") as fin, \
         open(out_path, "w", encoding="utf-8") as fout:

        current_section: list[str] = []

        for raw_line in fin:
            line = raw_line.rstrip("\n")
            current_section.append(line)

            if line == "end_of_record":
                total_sections += 1

                # Collect before-stats from this section
                for l in current_section:
                    if l.startswith("FNF:"):
                        m = re.match(r"^FNF:(\d+)", l)
                        if m:
                            total_fn_found_before += int(m.group(1))
                    elif l.startswith("FNH:"):
                        m = re.match(r"^FNH:(\d+)", l)
                        if m:
                            total_fn_hit_before += int(m.group(1))

                processed = process_section(current_section)
                fout.writelines(processed)
                current_section = []

                # Collect after-stats
                for l in processed:
                    l = l.rstrip("\n")
                    if l.startswith("FNF:"):
                        m = re.match(r"^FNF:(\d+)", l)
                        if m:
                            total_fn_found_after += int(m.group(1))
                    elif l.startswith("FNH:"):
                        m = re.match(r"^FNH:(\d+)", l)
                        if m:
                            total_fn_hit_after += int(m.group(1))

        # Flush any trailing lines outside a section (shouldn't happen)
        if current_section:
            fout.writelines(
                l if l.endswith("\n") else l + "\n" for l in current_section
            )

    return (
        total_sections,
        total_fn_found_before,
        total_fn_hit_before,
        total_fn_found_after,
        total_fn_hit_after,
    )


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} input.info [output.info]", file=sys.stderr)
        sys.exit(1)

    in_path = sys.argv[1]
    in_place = len(sys.argv) < 3
    out_path: str = sys.argv[2] if not in_place else ""

    if in_place:
        # Write to a temp file next to input, then rename atomically
        dir_name = os.path.dirname(os.path.abspath(in_path))
        tmp_fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".info.tmp")
        os.close(tmp_fd)
    else:
        tmp_path = out_path

    try:
        sections, fnf_before, fnh_before, fnf_after, fnh_after = process_info_file(
            in_path, tmp_path
        )

        if in_place:
            os.replace(tmp_path, in_path)

        pct_before = 100.0 * fnh_before / fnf_before if fnf_before else 0.0
        pct_after = 100.0 * fnh_after / fnf_after if fnf_after else 0.0
        newly_covered = fnh_after - fnh_before

        print(
            f"dedup_lcov_instantiations: {sections} sections processed\n"
            f"  Functions before: {fnh_before}/{fnf_before} ({pct_before:.1f}%)\n"
            f"  Functions after:  {fnh_after}/{fnf_after} ({pct_after:.1f}%)\n"
            f"  Newly covered:    +{newly_covered} functions"
        )

    except Exception:
        if in_place and tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise


if __name__ == "__main__":
    main()
