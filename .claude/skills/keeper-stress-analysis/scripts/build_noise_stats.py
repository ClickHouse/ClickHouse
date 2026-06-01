#!/usr/bin/env python3
"""Per-(scenario, backend, metric) noise characterization.

For each (scenario, backend) group in `merged_metrics.tsv`, computes the
median, sample stddev, coefficient of variation (`cv = stddev / |median|`),
and p95 of every metric in `_common.HEADLINE_METRICS` across the available
runs. Output: `noise_stats.tsv` — one row per (scenario, backend, metric).

The use case is calibration. The skill claims a ~5 % rps noise floor and
~10 % p99 noise floor (see `references/methodology.md`); this output lets
a future Claude verify those claims hold across the current dataset and
flag scenarios that are notably noisier than the others.

Skips (scenario, backend) pairs with `< 3` runs because stddev is only
meaningful with at least three points (matching the spirit of
`build_cumulative_gains.py`'s `< 2 * WINDOW_SIZE` skip, but lower because
this is descriptive rather than median-of-3-vs-3).
"""
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from _common import HEADLINE_METRICS, to_float

ROOT = Path(__file__).parent

MIN_RUNS = 3


def main():
    by_sb = defaultdict(list)  # (scenario, backend) -> list of row dicts
    with open(ROOT / "merged_metrics.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            by_sb[(r["scenario"], r["backend"])].append(r)

    out_path = ROOT / "noise_stats.tsv"
    cols = ["scenario", "backend", "metric", "n_runs",
            "median", "stddev", "cv", "p95"]
    skipped = []
    with open(out_path, "w", newline="") as fout:
        w = csv.writer(fout, delimiter="\t", lineterminator="\n")
        w.writerow(cols)
        for (sc, be), rows in sorted(by_sb.items()):
            if len(rows) < MIN_RUNS:
                skipped.append((sc, be, len(rows)))
                continue
            for metric, _ in HEADLINE_METRICS:
                # Include zeros: for non-negative metrics like `error_pct`,
                # zero is the dominant baseline. Excluding it would bias the
                # noise sample upward and make a stable metric look noisy.
                # Only `None` (parse failure / missing column) is dropped.
                values = [v for v in (to_float(r.get(metric)) for r in rows)
                          if v is not None and v >= 0]
                if len(values) < MIN_RUNS:
                    continue
                med = statistics.median(values)
                # `len(values) >= MIN_RUNS = 3` here, so `stdev` is always defined.
                sd = statistics.stdev(values)
                # `cv` is undefined when the median is exactly zero (which
                # is common for `error_pct` on healthy windows). Emit "" to
                # mark "no scale to normalise against" rather than 0.
                cv = (sd / abs(med)) if med > 0 else None
                # `quantiles(n=20)` returns 19 cut points splitting the data
                # into 20 equal buckets; index 18 is the cut between buckets
                # 19 and 20 → the 95th percentile. For `len < 4` we don't have
                # enough points to estimate a p95, so fall back to `max`.
                p95 = (statistics.quantiles(values, n=20)[18]
                       if len(values) >= 4 else max(values))
                w.writerow([
                    sc, be, metric, len(values),
                    f"{med:.4f}", f"{sd:.4f}",
                    f"{cv:.4f}" if cv is not None else "",
                    f"{p95:.4f}",
                ])
    print(f"Wrote {out_path}", file=sys.stderr)
    if skipped:
        print(
            f"Skipped {len(skipped)} (scenario, backend) pair(s) "
            f"with < {MIN_RUNS} runs:",
            file=sys.stderr,
        )
        for sc, be, n in skipped:
            print(f"  {sc} [{be}]: {n} run(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
