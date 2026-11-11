#!/usr/bin/env python3
"""Single CLI entrypoint for the keeper-stress-analysis pipeline.

Usage:
    python3 keeper_stress.py <subcommand> [args...]

Subcommands:
    merge          Build `merged_metrics.tsv` from the five staging TSVs.
                   (See `build_metrics_table.py`.)
    prmap          Build `pr_to_nightly.tsv` from `pr_meta.tsv` + bench summary.
                   (See `build_pr_nightly_map.py`.) Honors `KEEPER_SKILL_THRESHOLD`.
    deltas         Compute per-PR scenario deltas, per-PR summary, and
                   per-nightly summary. (See `compute_deltas.py`.) Honors
                   `KEEPER_SKILL_THRESHOLD`.
    prmetrics      Build `per_pr_metrics_long.tsv` (flat per-PR-metric form).
                   (See `build_per_pr_metrics_tsv.py`.)
    prisol         Build `pr_branch_isolated.tsv` (PR-branch pool isolation).
                   (See `build_pr_branch_isolated.py`.)
    cumulative     Build `cumulative_gains.tsv` and the summary pivot.
                   (See `build_cumulative_gains.py`.)
    diff A B       Build `commit_diff.tsv` comparing two arbitrary SHAs
                   (8-char prefix or full). (See `build_commit_diff.py`.)
    noise          Build `noise_stats.tsv` with per-(scenario, backend, metric)
                   median / stddev / cv / p95 across all runs in the window.
                   (See `build_noise_stats.py`.)

Each subcommand operates on files in this script's directory (where
`rebuild.sh` copies the SQL queries and scripts before running).
"""
import sys

import build_commit_diff
import build_cumulative_gains
import build_metrics_table
import build_noise_stats
import build_per_pr_metrics_tsv
import build_pr_branch_isolated
import build_pr_nightly_map
import compute_deltas


def _deltas(_args):
    """`compute_deltas.py` exposes two entrypoints; run both."""
    compute_deltas.compute_pr_deltas()
    compute_deltas.compute_nightly_summary()


def _diff(args):
    if len(args) != 2:
        print("usage: keeper_stress.py diff <shaA> <shaB>", file=sys.stderr)
        return 2
    return build_commit_diff.main(args[0], args[1])


SUBCOMMANDS = {
    "merge":      lambda args: build_metrics_table.main(),
    "prmap":      lambda args: build_pr_nightly_map.main(),
    "deltas":     _deltas,
    "prmetrics":  lambda args: build_per_pr_metrics_tsv.main(),
    "prisol":     lambda args: build_pr_branch_isolated.main(),
    "cumulative": lambda args: build_cumulative_gains.main(),
    "diff":       _diff,
    "noise":      lambda args: build_noise_stats.main(),
}


def _usage():
    names = " ".join(sorted(SUBCOMMANDS))
    return f"usage: keeper_stress.py <{names.replace(' ', '|')}> [args...]"


def main(argv):
    if len(argv) < 2 or argv[1] not in SUBCOMMANDS:
        print(_usage(), file=sys.stderr)
        return 2
    rc = SUBCOMMANDS[argv[1]](argv[2:])
    return rc or 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
