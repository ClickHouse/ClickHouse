#!/usr/bin/env python3
"""Human-readable summary of a (possibly still-growing) sweep results CSV.

Produces: overall win/loss with speedup stats, a bp x pp winner/speedup
matrix (averaged over whatever D/ratio points are done so far), and the
most recent N rows. Meant to be re-run at each progress checkpoint.
"""

from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict


def load_rows(path: str) -> list[dict[str, str]]:
    with open(path, encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fmt(x: float) -> str:
    return f"{x:.2f}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path")
    parser.add_argument("--recent", type=int, default=8)
    args = parser.parse_args(argv)

    rows = [row for row in load_rows(args.csv_path) if row["point_status"] == "OK"]
    if not rows:
        print("no completed OK points yet", file=sys.stderr)
        return 0

    radix_speedups = [
        float(r["speedup"]) for r in rows if r["winner"] == "radix_join" and r["speedup"]
    ]
    parallel_speedups = [
        float(r["speedup"]) for r in rows if r["winner"] == "parallel_hash" and r["speedup"]
    ]

    print(f"Completed OK points: {len(rows)}")
    print(
        f"radix_join wins: {len(radix_speedups)}  "
        f"(median {fmt(statistics.median(radix_speedups))}x, "
        f"max {fmt(max(radix_speedups))}x)"
        if radix_speedups
        else "radix_join wins: 0"
    )
    print(
        f"parallel_hash wins: {len(parallel_speedups)}  "
        f"(median {fmt(statistics.median(parallel_speedups))}x, "
        f"max {fmt(max(parallel_speedups))}x)"
        if parallel_speedups
        else "parallel_hash wins: 0"
    )

    radix_mem = [float(r["radix_peak_mem_mb"]) for r in rows if r["radix_peak_mem_mb"]]
    parallel_mem = [
        float(r["parallel_peak_mem_mb"]) for r in rows if r["parallel_peak_mem_mb"]
    ]
    if radix_mem or parallel_mem:
        print(f"\nPeak memory (MB), points with data: radix={len(radix_mem)} parallel={len(parallel_mem)}")
        if radix_mem:
            print(
                f"  radix_join:    median {statistics.median(radix_mem):>10.1f}  "
                f"max {max(radix_mem):>10.1f}"
            )
        if parallel_mem:
            print(
                f"  parallel_hash: median {statistics.median(parallel_mem):>10.1f}  "
                f"max {max(parallel_mem):>10.1f}"
            )
        paired = [
            (float(r["radix_peak_mem_mb"]), float(r["parallel_peak_mem_mb"]))
            for r in rows
            if r["radix_peak_mem_mb"] and r["parallel_peak_mem_mb"]
        ]
        if paired:
            ratios = [radix / parallel for radix, parallel in paired if parallel > 0]
            if ratios:
                print(
                    f"  radix/parallel memory ratio over {len(ratios)} paired points: "
                    f"median {statistics.median(ratios):.2f}x "
                    f"(>1 means radix_join uses MORE memory)"
                )
    else:
        print(
            "\nPeak memory: no data yet (either not measured for any point in "
            "this file, or --output was built before MemoryTrackerPeakUsage "
            "capture was added to the tool)"
        )

    # bp x pp matrix: average signed log-speedup so + means radix favored.
    cell_speedups: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in rows:
        if not r["speedup"] or r["winner"] not in ("radix_join", "parallel_hash"):
            continue
        signed = float(r["speedup"]) if r["winner"] == "radix_join" else -float(r["speedup"])
        cell_speedups[(r["bp"], r["pp"])].append(signed)

    bps = sorted({r["bp"] for r in rows}, key=int)
    pps = sorted({r["pp"] for r in rows}, key=int)
    print("\nbp x pp matrix (avg signed speedup; +N = radix_join N x faster, -N = parallel_hash N x faster; blank = no data yet):")
    header = "bp\\pp".ljust(8) + "".join(f"pp={pp}".rjust(10) for pp in pps)
    print(header)
    for bp in bps:
        line = f"bp={bp}".ljust(8)
        for pp in pps:
            values = cell_speedups.get((bp, pp))
            if not values:
                line += "-".rjust(10)
            else:
                avg = sum(values) / len(values)
                line += f"{avg:+.2f}({len(values)})".rjust(10)
        print(line)

    print(f"\nMost recent {args.recent} points:")
    for r in rows[-args.recent :]:
        winner = r["winner"] or "?"
        speedup = f"{r['speedup']}x" if r["speedup"] else "-"
        print(
            f"  D={int(r['D']):>10} ratio={r['ratio']} bp={r['bp']} pp={r['pp']} "
            f"-> {winner} ({speedup})  radix={r['radix_median_ms']}ms "
            f"parallel={r['parallel_median_ms']}ms"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
