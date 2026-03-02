import os
from ci.praktika.utils import Utils
import re
from collections import defaultdict
from ci.praktika.result import Result

repo_root = Utils.cwd()
temp_dir = f"{repo_root}/ci/tmp/"

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
        print("No changed-line hunks found (is your diff empty?).")
        raise SystemExit(0)

    # helper: does absolute SF path correspond to repo-relative path?
    def sf_matches(sf, rel):
        return sf.endswith("/" + rel) or sf.endswith(rel)

    # --- stream parse LCOV .info: only DA lines; count only changed lines
    total = covered = 0
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

                total += 1
                if cnt > 0:
                    covered += 1
                else:
                    uncovered.append((active_rel, ln))

    if total == 0:
        print(
            "PR changed-lines coverage: N/A (no coverable changed lines found in .info)."
        )
        raise SystemExit(0)

    pct = 100.0 * covered / total

    CONTEXT = 2  # lines before/after
    MAX_PRINT = 200  # max uncovered lines to print total

    msg = f"PR changed-lines coverage: {pct:.2f}% ({covered}/{total})"
    print(msg)

    if uncovered:
        print("\nUncovered changed code (with context):\n")

        # group uncovered lines by file
        by_file = defaultdict(list)
        for rel, ln in uncovered[:MAX_PRINT]:
            by_file[rel].append(ln)

        for rel in sorted(by_file.keys()):
            abs_path = os.path.join(repo_root, rel)

            print("=" * 80)
            print(rel)
            print("=" * 80)

            if not os.path.exists(abs_path):
                print(f"  [source file not found: {abs_path}]")
                continue

            with open(abs_path, encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

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

    r = Result.create_from(
        name="Print Uncovered Code",
        status=Result.Status.SUCCESS,
        info=msg,
        with_info_from_results=True,
    )
    r.set_comment(msg)
    r.complete_job()
