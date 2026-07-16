#!/usr/bin/env python3
"""Parse ClickHouse tests/performance/scripts/perf.py TSV-ish output."""
from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any


def pct(x: float) -> str:
    return f"{x * 100:+.2f}%"


def sec(x: Any) -> str:
    try:
        return f"{float(x):.6g}s"
    except Exception:
        return "—"


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    frac = pos - lo
    return xs[lo] * (1 - frac) + xs[hi] * frac


def verdict(diff: float, pvalue: float, threshold: float) -> str:
    if abs(diff) < threshold:
        return "within threshold"
    if pvalue > 0.05:
        return "not statistically significant"
    return "candidate slower" if diff > 0 else "candidate faster"


def parse(path: Path) -> dict[str, Any]:
    threshold = 0.05
    display: dict[int, str] = {}
    query_runs: dict[int, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    diffs = []
    medians = {}
    servers = []
    stages = []
    skipped = []
    partial = []
    profile_total = None

    for raw in path.read_text(errors="replace").splitlines():
        if not raw.strip():
            continue
        parts = raw.rstrip("\n").split("\t")
        tag = parts[0]
        try:
            if tag == "report-threshold" and len(parts) >= 2:
                threshold = float(parts[1])
            elif tag == "display-name" and len(parts) >= 3:
                display[int(parts[1])] = parts[2].replace("\\n", "\n").replace("\\t", "\t")
            elif tag == "server" and len(parts) >= 6:
                servers.append(parts[1:])
            elif tag == "stage":
                stages.append(parts[1:])
            elif tag == "skipped":
                skipped.append("\t".join(parts[1:]))
            elif tag == "partial" and len(parts) >= 3:
                partial.append((int(parts[1]), parts[2]))
            elif tag == "query" and len(parts) >= 5:
                qidx = int(parts[1])
                conn = int(parts[3])
                elapsed = float(parts[4])
                query_runs[qidx][conn].append(elapsed)
            elif tag == "median" and len(parts) >= 3:
                medians[int(parts[1])] = float(parts[2])
            elif tag == "diff" and len(parts) >= 6:
                qidx = int(parts[1])
                old = float(parts[2])
                new = float(parts[3])
                rel = float(parts[4])
                pvalue = float(parts[5])
                diffs.append({"query": qidx, "old": old, "new": new, "diff": rel, "pvalue": pvalue})
            elif tag == "profile-total" and len(parts) >= 2:
                profile_total = float(parts[1])
        except Exception:
            # Keep parser forgiving; malformed lines are ignored.
            pass

    return {
        "threshold": threshold,
        "display": display,
        "query_runs": query_runs,
        "diffs": diffs,
        "medians": medians,
        "servers": servers,
        "stages": stages,
        "skipped": skipped,
        "partial": partial,
        "profile_total": profile_total,
    }


def print_report(path: Path, data: dict[str, Any]) -> None:
    print(f"# perf.py result: `{path}`\n")
    print(f"Report threshold: `{pct(data['threshold'])}`\n")
    if data["servers"]:
        print("## Servers\n")
        print("| Index | Host | Port | User | TLS |")
        print("|---:|---|---:|---|---|")
        for s in data["servers"]:
            # perf.py: server idx host port user ssl_status
            print(f"| {s[0]} | `{s[1]}` | {s[2]} | `{s[3]}` | {s[4]} |")
        print()

    if data["skipped"]:
        print("## Skipped\n")
        for s in data["skipped"]:
            print(f"- {s}")
        print()

    print("## Diff rows\n")
    if not data["diffs"]:
        print("No `diff` rows. Either only one server was used, too few samples were collected, or no query passed perf.py's diff/p-value gate.\n")
    else:
        print("| Query | Old median | New median | Diff | p-value | Verdict | Query preview |")
        print("|---:|---:|---:|---:|---:|---|---|")
        for d in sorted(data["diffs"], key=lambda x: abs(x["diff"]), reverse=True):
            qidx = d["query"]
            preview = (data["display"].get(qidx) or "").replace("\n", " ")[:160]
            print(
                f"| {qidx} | {sec(d['old'])} | {sec(d['new'])} | {pct(d['diff'])} | "
                f"{d['pvalue']:.4g} | {verdict(d['diff'], d['pvalue'], data['threshold'])} | `{preview}` |"
            )
        print()

    print("## Per-query sample summary\n")
    print("| Query | Server | Runs | p05 | p50 | p95 | Min | Max |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for qidx in sorted(data["query_runs"]):
        for conn in sorted(data["query_runs"][qidx]):
            vals = data["query_runs"][qidx][conn]
            print(
                f"| {qidx} | {conn} | {len(vals)} | {sec(percentile(vals, 0.05))} | "
                f"{sec(percentile(vals, 0.50))} | {sec(percentile(vals, 0.95))} | {sec(min(vals))} | {sec(max(vals))} |"
            )
    print()

    if data["partial"]:
        print("## Partial queries\n")
        for qidx, conns in data["partial"]:
            print(f"- query {qidx}: ran only on connections `{conns}`")
        print()

    if data["profile_total"] is not None:
        print(f"Profile total seconds: `{data['profile_total']:.3f}`\n")



def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path", type=Path)
    args = ap.parse_args()
    print_report(args.path, parse(args.path))


if __name__ == "__main__":
    main()
