"""Shared helpers used by every script in this skill.

Lives next to the per-step scripts so they can `from _common import …`. Anything
that previously had to be defined in three files (the fault/no-fault matcher,
`to_float`, `iso_week`, `classify`, the `KEEPER_SKILL_THRESHOLD` window edge)
is consolidated here so a fix in one place applies everywhere — and the test
harness can exercise the same definitions the runtime uses.
"""
import datetime
import os
from pathlib import Path


# Lower-bound `ts` for which PRs are considered in-window, parsed once.
# Set by `rebuild.sh` from its `$2` arg via the `KEEPER_SKILL_THRESHOLD` env
# var; defaults to `2026-03-25` (when the current keeper-stress framework
# went live). The default lives here exactly once — `rebuild.sh` carries the
# bash-side default for the env var, but the Python-side default is unique
# to this module so a future bump can't drift between scripts.
_threshold_str = os.environ.get("KEEPER_SKILL_THRESHOLD", "2026-03-25")
KEEPER_SKILL_THRESHOLD = datetime.datetime.fromisoformat(_threshold_str).replace(
    tzinfo=datetime.timezone.utc
)

# Upper-bound `ts` for the analysis window (exclusive). Set by `rebuild.sh`
# from its `$3` arg via `KEEPER_SKILL_THRESHOLD_END`. Defaults to a far-future
# sentinel (`9999-12-31`) so single-arg invocations preserve the original
# "everything from THRESHOLD onwards" behavior.
_threshold_end_str = os.environ.get("KEEPER_SKILL_THRESHOLD_END", "9999-12-31")
KEEPER_SKILL_THRESHOLD_END = datetime.datetime.fromisoformat(_threshold_end_str).replace(
    tzinfo=datetime.timezone.utc
)


def to_float(s):
    """Parse a TSV cell to float; return None for empty / non-numeric."""
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def is_fault_scenario(scenario):
    """True if the scenario tag is a fault sweep (`prod-mix-fault[default]`),
    False for no-fault (`prod-mix-no-fault[default]`).

    The naive `"fault" in scenario` test would match both because `no-fault`
    contains `fault`. Anchoring on the literal `-fault[` and `-no-fault[`
    substrings is the disambiguator.
    """
    return "-fault[" in scenario and "-no-fault[" not in scenario


def iso_week(dt):
    """Return the ISO-week key `YYYY-WNN` for a `datetime`. `isocalendar` does
    the year/W01/W52-53 boundary handling correctly; do *not* arithmetic the
    formatted string.
    """
    yr, wk, _ = dt.isocalendar()
    return f"{yr}-W{wk:02d}"


def common_prefix(a, b):
    """Length-aware longest common prefix of two strings. Used by SHA-typo
    suggestions; promoted here so it's testable and reusable.
    """
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return a[:i]


# Headline metrics to track per scenario+backend, paired with "good direction".
# The order here is the canonical column order used by `compute_deltas` when
# emitting per-PR delta rows.
HEADLINE_METRICS = [
    ("rps",                          "higher_better"),
    ("read_p99_ms",                  "lower_better"),
    ("write_p99_ms",                 "lower_better"),
    ("error_pct",                    "lower_better"),
    ("peak_mem_gb",                  "lower_better"),
    ("p95_cpu_cores",                "lower_better"),
    ("zk_max_latency_max",           "lower_better"),
    ("FileSync_us_per_s_avg",        "lower_better"),
    ("StorageLockWait_us_per_s_avg", "lower_better"),
    ("OutstandingRequests_max",      "lower_better"),
]
HEADLINE_LOOKUP = dict(HEADLINE_METRICS)

# Significance bands per metric, in percent. Must mirror the rubric in
# `references/methodology.md`. Format: metric -> (clean_band, watch_band).
# Throughput metrics are tighter (5/15) because the bench is repeatable;
# tail latency and memory are wider (10/30) because they're noisier.
CLASSIFY_BANDS = {
    "rps":          (5, 15),
    "read_rps":     (5, 15),
    "write_rps":    (5, 15),
    "read_p99_ms":  (10, 30),
    "write_p99_ms": (10, 30),
    "peak_mem_gb":  (10, 30),
    "avg_mem_gb":   (10, 30),
}
DEFAULT_BANDS = (5, 15)


def classify(metric, pre, post):
    """Verdict for a single metric Δ. Returns one of `clean`, `watch`,
    `regression`, `no-data`. Bands per metric come from `CLASSIFY_BANDS`;
    `error_pct` is special-cased to absolute percentage-point thresholds
    (matching the methodology rubric, where `error_pct` <0.05 PP is noise).
    """
    if pre is None or post is None:
        return "no-data"
    if metric == "error_pct":
        if pre == 0 and post == 0:
            return "clean"
        if post < pre + 0.05:
            return "clean"
        if post < pre + 0.5:
            return "watch"
        return "regression"
    if pre == 0 and post == 0:
        return "clean"
    if pre == 0:
        return "watch"
    pct = (post - pre) / abs(pre) * 100.0
    direction = HEADLINE_LOOKUP.get(metric, "lower_better")
    clean_band, watch_band = CLASSIFY_BANDS.get(metric, DEFAULT_BANDS)
    if direction == "higher_better":
        if pct >= -clean_band: return "clean"
        if pct >= -watch_band: return "watch"
        return "regression"
    else:
        if pct <= clean_band: return "clean"
        if pct <= watch_band: return "watch"
        return "regression"


def require_cols(reader, names, source=None):
    """Raise `ValueError` if `reader.fieldnames` is missing any of `names`.

    Used right after constructing a `csv.DictReader` so a renamed SQL column
    fails loudly at load time instead of silently producing empty values
    downstream. `source` is an optional path / label for the error message.
    """
    fields = reader.fieldnames or []
    missing = [c for c in names if c not in fields]
    if missing:
        prefix = f"{Path(source).name}: " if source else ""
        raise ValueError(
            f"{prefix}missing expected columns {missing}; header is {fields}"
        )


def parse_merged_at(row, source=None):
    """Parse a `pr_meta.tsv` row's `mergedAt` field into a tz-aware UTC datetime.

    Returns `None` for unmerged PRs (empty `mergedAt`) so callers can skip them
    with a clear log line — `gh pr list` will return open PRs with empty
    `mergedAt` and `datetime.fromisoformat("")` would otherwise abort the whole
    pipeline. Both `build_pr_nightly_map` and `compute_deltas`'s nightly-summary
    loader share this contract.
    """
    raw = (row.get("mergedAt") or "").strip()
    if not raw:
        return None
    return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
