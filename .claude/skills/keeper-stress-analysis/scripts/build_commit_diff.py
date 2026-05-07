#!/usr/bin/env python3
"""Per-(scenario, backend, metric) Δ between two arbitrary commit SHAs.

Use case: "compare commit `abcd1234` vs `efgh5678` directly" — the two SHAs
need not be a PR's pre/post pair. For PR validation, use `compute_deltas`
instead (it handles the kind-matched fallback baseline; see methodology.md).

Output: `commit_diff.tsv` with columns
    scenario, backend, kind, metric, sha_a, sha_b,
    value_a, value_b, delta_abs, delta_pct, verdict

`verdict` reuses `_common.classify` so the rubric matches every other
per-metric Δ output in the skill.
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

from _common import HEADLINE_METRICS, classify, common_prefix, is_fault_scenario, to_float

ROOT = Path(__file__).parent


def _resolve_sha(prefix, by_sha8):
    """Resolve a user-provided SHA to the 8-char form present in the index.

    Accepts an 8-char prefix or a longer SHA; case-insensitive.
    """
    p = prefix.lower()[:8]
    if p in by_sha8:
        return p
    return None


def main(sha_a_arg, sha_b_arg):
    by_sha8 = defaultdict(dict)  # sha8 -> {(scenario, backend) -> row}
    with open(ROOT / "merged_metrics.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            sha8 = r["commit_sha"][:8].lower()
            by_sha8[sha8][(r["scenario"], r["backend"])] = r

    sha_a = _resolve_sha(sha_a_arg, by_sha8)
    sha_b = _resolve_sha(sha_b_arg, by_sha8)
    if sha_a is None or sha_b is None:
        # Suggest by-prefix-similarity rather than alphabetical order so a
        # typo like `a1b2c3d5` -> `a1b2c3d4` actually surfaces the close match.
        def _suggest(arg):
            p = arg.lower()[:8]
            scored = sorted(
                by_sha8.keys(),
                key=lambda k: (-len(common_prefix(k, p)), k),
            )
            return scored[:5]
        miss = []
        if sha_a is None:
            miss.append(f"{sha_a_arg!r} → suggestions: {_suggest(sha_a_arg)}")
        if sha_b is None:
            miss.append(f"{sha_b_arg!r} → suggestions: {_suggest(sha_b_arg)}")
        print(
            "build_commit_diff: SHA not found in merged_metrics.tsv:\n  "
            + "\n  ".join(miss),
            file=sys.stderr,
        )
        return 2

    sb_a = set(by_sha8[sha_a].keys())
    sb_b = set(by_sha8[sha_b].keys())
    shared = sorted(sb_a & sb_b)
    only_a = sorted(sb_a - sb_b)
    only_b = sorted(sb_b - sb_a)
    if only_a or only_b:
        print(
            f"build_commit_diff: {len(only_a)} (scenario,backend) pair(s) "
            f"only in {sha_a}, {len(only_b)} only in {sha_b}; emitting "
            f"the {len(shared)}-pair intersection.",
            file=sys.stderr,
        )

    out_path = ROOT / "commit_diff.tsv"
    cols = ["scenario", "backend", "kind", "metric", "sha_a", "sha_b",
            "value_a", "value_b", "delta_abs", "delta_pct", "verdict"]
    with open(out_path, "w", newline="") as fout:
        w = csv.writer(fout, delimiter="\t", lineterminator="\n")
        w.writerow(cols)
        for sc, be in shared:
            ra = by_sha8[sha_a][(sc, be)]
            rb = by_sha8[sha_b][(sc, be)]
            kind = "fault" if is_fault_scenario(sc) else "no-fault"
            for metric, _ in HEADLINE_METRICS:
                va = to_float(ra.get(metric))
                vb = to_float(rb.get(metric))
                if va is None and vb is None:
                    continue
                delta_abs = "" if (va is None or vb is None) else f"{vb - va:.4f}"
                # delta_pct semantics:
                #   missing data → "" (don't fabricate a percentage)
                #   0 → 0         → "" (no change, no scale to report against)
                #   0 → positive  → "inf" (unbounded relative change)
                #   else          → standard (post - pre) / |pre| * 100
                if va is None or vb is None:
                    delta_pct = ""
                elif va == 0:
                    delta_pct = "" if vb == 0 else "inf"
                else:
                    delta_pct = f"{(vb - va) / abs(va) * 100:.2f}"
                w.writerow([
                    sc, be, kind, metric, sha_a, sha_b,
                    "" if va is None else f"{va:.4f}",
                    "" if vb is None else f"{vb:.4f}",
                    delta_abs, delta_pct,
                    classify(metric, va, vb),
                ])
    print(f"Wrote {out_path} (sha_a={sha_a}, sha_b={sha_b}, {len(shared)} pairs)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: build_commit_diff.py <shaA> <shaB>", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]) or 0)
