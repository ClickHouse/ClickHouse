#!/usr/bin/env python3
"""Small helper for the ClickHouse performance.ci API.

This is intentionally dependency-free: Python stdlib only.
It prints markdown-ish summaries for agents/humans.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import subprocess
from datetime import datetime, timezone
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Any

BASE_DEFAULT = "https://performance.ci.clickhouse.com/api/v1"
PLAY_DEFAULT = "https://play.clickhouse.com/"
DEFAULT_METRICS = "client_time,real_time,cpu_time,memory"


def q(s: Any) -> str:
    return urllib.parse.quote(str(s), safe="")


def fetch_json(base: str, path: str, params: dict[str, Any] | None = None) -> Any:
    url = base.rstrip("/") + path
    if params:
        clean = {k: v for k, v in params.items() if v is not None}
        if clean:
            url += "?" + urllib.parse.urlencode(clean, doseq=True)
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "clickhouse-performance-analysis-skill"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def api_url(base: str, path: str, params: dict[str, Any] | None = None) -> str:
    url = base.rstrip("/") + path
    if params:
        clean = {k: v for k, v in params.items() if v is not None}
        if clean:
            url += "?" + urllib.parse.urlencode(clean, doseq=True)
    return url


def change_pct(x: Any) -> str:
    if x is None:
        return "—"
    try:
        v = float(x)
    except Exception:
        return str(x)
    # Observed API values are fractional: 0.232 means +23.2%.
    if abs(v) <= 10:
        return f"{v * 100:+.2f}%"
    return f"{v:+.2f}%"


def num(x: Any) -> str:
    if x is None:
        return "—"
    try:
        v = float(x)
    except Exception:
        return str(x)
    if abs(v) >= 1000:
        return f"{v:.0f}"
    if abs(v) >= 10:
        return f"{v:.3f}"
    return f"{v:.6g}"


def over_threshold(row: dict[str, Any]) -> str:
    if row.get("diffPercent") is None or row.get("statThreshold") is None:
        return "—"
    try:
        diff = abs(float(row.get("diffPercent")))
        threshold = abs(float(row.get("statThreshold")))
    except Exception:
        return "—"
    # API floats are often rounded in display but differ by tiny binary/decimal error.
    return "yes" if diff + 1e-6 >= threshold else "no"


def optional_fetch_json(base: str, path: str, params: dict[str, Any] | None = None) -> tuple[Any | None, str | None]:
    try:
        return fetch_json(base, path, params), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def sql_quote(value: Any) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def clickhouse_play_json_each_row(sql: str, play_url: str = PLAY_DEFAULT, user: str = "explorer") -> list[dict[str, Any]]:
    url = play_url.rstrip("/") + "/?" + urllib.parse.urlencode({"user": user})
    req = urllib.request.Request(
        url,
        data=sql.encode(),
        method="POST",
        headers={"User-Agent": "clickhouse-performance-analysis-skill"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        text = r.read().decode()
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def shorten(text: Any, max_len: int = 170) -> str:
    if text is None:
        return "—"
    value = " ".join(str(text).split())
    if len(value) <= max_len:
        return value
    return value[: max_len - 1].rstrip() + "…"


def module_fact_summary(module: dict[str, Any], max_items: int = 4) -> str:
    facts = []
    for item in module.get("rows") or []:
        label = item.get("label") or ""
        value = item.get("value")
        if not label or label.lower() == "status":
            continue
        facts.append(f"{label}: {value}")
    if not facts:
        return "—"
    return "; ".join(facts[:max_items])


def confidence_overall(row: dict[str, Any]) -> tuple[str, str]:
    conf = row.get("confidence") or {}
    tier = conf.get("tier") or "scored"
    reason = conf.get("reason") or "no overall reason returned"
    # The current dashboard confidence vocabulary is slowdown/regression-oriented.
    # Do not print slowdown/regression wording on a speedup row in a run summary.
    if row.get("direction") == "speedup":
        if "regression" in tier:
            tier = "stable_speedup"
        reason = reason.replace("slowdown", "change").replace("regression", "change")
    return tier, reason


def evidence_class(row: dict[str, Any]) -> str:
    conf = row.get("confidence") or {}
    raw_tier = str(conf.get("tier") or "").lower()
    raw_reason = str(conf.get("reason") or "").lower()
    if row.get("bucket") == "high-noise/unstable-threshold":
        return "HIGH-NOISE"
    tier, reason = confidence_overall(row) if conf else ("", "")
    tier_l = tier.lower()
    reason_l = reason.lower() or raw_reason
    # Tier is authoritative. Reasons can mention "history noise" even for confirmed rows.
    if "noise" in tier_l:
        return "NOISE"
    if row.get("direction") == "speedup" and ("stable" in tier_l or "confirmed" in tier_l or "stable" in raw_tier or "confirmed" in raw_tier):
        return "STABLE SPEEDUP"
    if row.get("direction") == "slowdown" and ("confirmed" in tier_l or "regression" in tier_l or "confirmed" in raw_tier or "regression" in raw_tier):
        return "SLOWDOWN SIGNAL"
    if tier_l or raw_tier:
        return "SUSPECT"
    if "noise" in reason_l:
        return "NOISE"
    return "UNSCORED"


def is_noise_or_uncertain(row: dict[str, Any]) -> bool:
    return evidence_class(row) in {"NOISE", "HIGH-NOISE", "UNSCORED"}


def display_confidence_tier(row: dict[str, Any], tier: str) -> str:
    tier_l = str(tier or "").lower()
    if row.get("direction") == "slowdown" and ("confirmed" in tier_l or "regression" in tier_l):
        return "supported slowdown"
    if row.get("direction") == "speedup" and ("stable" in tier_l or "confirmed" in tier_l):
        return "supported speedup"
    return str(tier or "scored")


def confidence_status(row: dict[str, Any]) -> str:
    conf = row.get("confidence") or {}
    prefix = evidence_class(row)
    if conf.get("tier") or conf.get("reason"):
        tier, reason = confidence_overall(row)
        return f"**{prefix}** — {display_confidence_tier(row, tier)}: {shorten(reason, 95)}"
    modules = conf.get("modules") or []
    if not modules:
        return "—"
    useful = []
    for m in modules:
        status = m.get("status")
        if status and status != "neutral":
            title = m.get("title") or m.get("code") or "module"
            useful.append(f"{title}: {status}")
    return f"**{prefix}** — {shorten('; '.join(useful) if useful else 'scored, no decisive module', 120)}"


def print_confidence_details(rows: list[dict[str, Any]]) -> None:
    seen = set()
    any_conf = False
    for r in rows:
        conf = r.get("confidence") or {}
        modules = conf.get("modules") or []
        if not conf:
            continue
        key = row_key(r)
        if key in seen:
            continue
        seen.add(key)
        any_conf = True
        tier, reason = confidence_overall(r)
        print(f"\n### Confidence explanation: `{r.get('test','—')}/{r.get('queryIndex','—')}` `{r.get('metric','—')}/{r.get('arch','—')}`\n")
        print(f"Overall: **{display_confidence_tier(r, tier)}** — {reason}\n")
        if modules:
            print("| Evidence check | Status | Useful facts | Interpretation |")
            print("|---|---|---|---|")
            for m in modules:
                title = m.get("title") or m.get("code") or "module"
                print(f"| {title} | {m.get('status','—')} | {shorten(module_fact_summary(m), 180)} | {shorten(m.get('interpretation'), 220)} |")
    if not any_conf:
        print("\nNo confidence details returned for the selected row(s).")

def row_key(row: dict[str, Any]) -> tuple:
    query_index = row.get("queryIndex")
    if query_index is None:
        query_index = -1
    return (row.get("arch") or "", row.get("test") or "", query_index, row.get("metric") or "")


def rows_for_display(rows: list[dict[str, Any]], limit: int | None = None) -> list[dict[str, Any]]:
    rows = sorted(rows, key=lambda r: abs(float(r.get("diffPercent") or 0)), reverse=True)
    return rows[:limit] if limit else rows


def dashboard_link_for_row(run_id: str | None, row: dict[str, Any]) -> str:
    if not run_id:
        return "—"
    test = row.get("test")
    query_index = row.get("queryIndex")
    if test is None or query_index is None:
        return "—"
    return f"[open]({dashboard_query_url(run_id, test, query_index, row.get('metric'))})"


def dashboard_test_query_link_for_row(run_id: str | None, row: dict[str, Any]) -> str:
    if not run_id:
        return "—"
    test = row.get("test")
    query_index = row.get("queryIndex")
    if test is None or query_index is None:
        return "—"
    return f"[test page]({dashboard_test_url(run_id, test, query_index, row.get('metric'))})"


def print_rows(title: str, rows: list[dict[str, Any]], limit: int | None = None, run_id: str | None = None) -> None:
    print(f"\n## {title}\n")
    if not rows:
        print("No rows.\n")
        return
    rows = rows_for_display(rows, limit)
    if run_id:
        print("| Direction | Arch | Test | Query | Dashboard | Metric | Old | New | Diff | Threshold | Over threshold | Confidence |")
        print("|---|---|---|---:|---|---|---:|---:|---:|---:|---|---|")
    else:
        print("| Direction | Arch | Test | Query | Metric | Old | New | Diff | Threshold | Over threshold | Confidence |")
        print("|---|---|---|---:|---|---:|---:|---:|---:|---|---|")
    for r in rows:
        common = (
            f"| {r.get('direction','—')} | {r.get('arch','—')} | `{r.get('test','—')}` | {r.get('queryIndex','—')} | "
        )
        if run_id:
            common += f"{dashboard_link_for_row(run_id, r)} | "
        print(
            common
            + f"`{r.get('metric','—')}` | {num(r.get('oldValue'))} | {num(r.get('newValue'))} | "
            f"{change_pct(r.get('diffPercent'))} | {change_pct(r.get('statThreshold'))} | {over_threshold(r)} | {confidence_status(r)} |"
        )
    print()


def cmd_runs(args: argparse.Namespace) -> None:
    search = args.pr if args.pr is not None else args.q
    data = fetch_json(args.base, "/runs", {"q": search, "metrics": args.metrics})
    items = data.get("items") or []
    if args.pr is not None:
        try:
            pr = int(args.pr)
            items = [x for x in items if (x.get("identity") or {}).get("prNumber") == pr]
        except Exception:
            pass
    print(f"API: {api_url(args.base, '/runs', {'q': search, 'metrics': args.metrics})}\n")
    print("| Time | Run ID | Dashboard | PR | Old | New | Arches | Changed | Slowdowns | Speedups | Reports |")
    print("|---|---|---|---:|---|---|---|---:|---:|---:|---:|")
    for item in items:
        ident = item.get("identity") or {}
        run_id = ident.get('runId', '—')
        print(
            f"| {ident.get('runTime','—')} | `{run_id}` | [open]({dashboard_run_url(run_id)}) | {ident.get('prNumber','—')} | "
            f"`{str(ident.get('oldSha',''))[:12]}` | `{str(ident.get('newSha',''))[:12]}` | "
            f"{','.join(item.get('arches') or [])} | {item.get('changedQueries','—')} | "
            f"{item.get('slowdownQueries','—')} | {item.get('speedupQueries','—')} | {item.get('reportCount','—')} |"
        )



def attach_confidence_for_display(args: argparse.Namespace, row_groups: list[list[dict[str, Any]]]) -> list[str]:
    if not getattr(args, "with_confidence", False):
        return []
    rows: list[dict[str, Any]] = []
    for group in row_groups:
        rows.extend(rows_for_display(group, args.limit))
    tests = sorted({r.get("test") for r in rows if r.get("test")})
    confidence_by_key: dict[tuple, dict[str, Any]] = {}
    errors: list[str] = []
    for test in tests:
        conf_path = f"/runs/{q(args.run_id)}/tests/{q(test)}/confidence"
        conf, err = optional_fetch_json(args.base, conf_path, {"metrics": args.metrics})
        if err:
            errors.append(f"{test}: {err}")
            continue
        for cr in (conf.get("changedMetrics") or []) + (conf.get("unstableMetrics") or []):
            if cr.get("confidence"):
                confidence_by_key[row_key(cr)] = cr.get("confidence")
    for r in rows:
        conf = confidence_by_key.get(row_key(r))
        if conf:
            r["confidence"] = conf
    return errors


def cmd_run(args: argparse.Namespace) -> None:
    data = fetch_json(args.base, f"/runs/{q(args.run_id)}", {"metrics": args.metrics})
    slowdowns = data.get("slowdowns") or []
    speedups = data.get("speedups") or []
    unstable = data.get("unstableQueries") or []
    confidence_errors = attach_confidence_for_display(args, [slowdowns, speedups, unstable])
    print(f"API: {api_url(args.base, f'/runs/{q(args.run_id)}', {'metrics': args.metrics})}\n")
    print(f"Dashboard: {dashboard_run_url(args.run_id)}\n")
    run = data.get("run") or {}
    print(f"# Run `{run.get('runId', args.run_id)}`\n")
    for card in data.get("summaryCards") or []:
        print(f"- {card.get('label')}: **{card.get('value')}** {card.get('detail') or ''}")
    if getattr(args, "with_confidence", False):
        print("\n_Confidence requested: fetched per-test confidence for displayed rows where available._")
        for err in confidence_errors[:5]:
            print(f"- confidence unavailable for {err}")
    print_rows("Slowdowns", slowdowns, args.limit, run_id=args.run_id)
    print_rows("Speedups", speedups, args.limit, run_id=args.run_id)
    if unstable:
        print("\n_Note: `unstableQueries` from the API are high-noise/high-threshold rows. They are useful for flakiness context, but many are not over the reporting threshold and may not be counted in the run summary cards._")
    print_rows("High-noise / unstable-threshold rows", unstable, args.limit, run_id=args.run_id)



def cmd_changes(args: argparse.Namespace) -> None:
    cmd_run(args)



def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


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


def period_text(times: list[datetime]) -> str:
    if not times:
        return "unknown period"
    start = min(times)
    end = max(times)
    delta = end - start
    if delta.days >= 2:
        span = f"{delta.days}d"
    else:
        span = f"{delta.total_seconds() / 3600:.1f}h"
    return f"{start.date()} → {end.date()} ({span})"


def point_pairs(points: list[dict[str, Any]]) -> list[tuple[datetime | None, float]]:
    out = []
    for p in points:
        value = p.get("value")
        if isinstance(value, (int, float)):
            out.append((parse_time(p.get("time")), float(value)))
    return out


def pct_summary(values: list[float]) -> str:
    if not values:
        return "no numeric values"
    return (
        f"p05={num(percentile(values, 0.05))}, "
        f"p25={num(percentile(values, 0.25))}, "
        f"p50={num(percentile(values, 0.50))}, "
        f"p75={num(percentile(values, 0.75))}, "
        f"p95={num(percentile(values, 0.95))}"
    )


def summarize_points(points: list[dict[str, Any]], recent_n: int = 30) -> str:
    pairs = point_pairs(points)
    if not pairs:
        return "No numeric points."
    pairs.sort(key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc))
    vals = [v for _, v in pairs]
    times = [t for t, _ in pairs if t is not None]
    recent = pairs[-min(recent_n, len(pairs)) :]
    recent_vals = [v for _, v in recent]
    recent_times = [t for t, _ in recent if t is not None]
    return (
        f"{len(vals)} points over {period_text(times)}; "
        f"all: {pct_summary(vals)}; "
        f"recent {len(recent_vals)} points over {period_text(recent_times)}: {pct_summary(recent_vals)}"
    )


def selected_value_context(rows: list[dict[str, Any]], points: list[dict[str, Any]], label: str) -> str:
    pairs = point_pairs(points)
    vals = [v for _, v in pairs]
    if not vals or not rows:
        return ""
    row = rows[0]
    old = row.get("oldValue")
    new = row.get("newValue")
    try:
        old_f = float(old)
        new_f = float(new)
    except Exception:
        return ""
    p50 = percentile(vals, 0.50)
    p95 = percentile(vals, 0.95)
    p05 = percentile(vals, 0.05)
    def ratio(v: float, denom: float) -> str:
        return "—" if not denom else f"{v / denom:.2f}x"
    return (
        f"Selected row vs {label}: old={num(old_f)} ({ratio(old_f, p50)} p50), "
        f"new={num(new_f)} ({ratio(new_f, p50)} p50, {ratio(new_f, p95)} p95; p05={num(p05)}, p95={num(p95)})."
    )


def change_points_summary(change_points: list[dict[str, Any]], limit: int = 8) -> str:
    if not change_points:
        return "No change points returned."
    seen = set()
    deduped = []
    for cp in change_points:
        key = (cp.get("time"), cp.get("direction"), cp.get("causalSha"), cp.get("magnitude"), cp.get("disparity"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cp)
    deduped.sort(key=lambda cp: parse_time(cp.get("time")) or datetime.min.replace(tzinfo=timezone.utc))
    times = [parse_time(cp.get("time")) for cp in deduped if parse_time(cp.get("time"))]
    lines = [f"{len(deduped)} unique change points over {period_text(times)}; latest {min(limit, len(deduped))}:"]
    for cp in deduped[-limit:]:
        lines.append(
            f"- {cp.get('time','—')} {cp.get('direction','—')} "
            f"magnitude={num(cp.get('magnitude'))} disparity={num(cp.get('disparity'))} "
            f"sha={str(cp.get('causalSha') or '')[:12] or '—'} trust={cp.get('trustLabel') or '—'}"
        )
    return "\n".join(lines)


def dashboard_run_url(run_id: str) -> str:
    return f"https://performance.ci.clickhouse.com/runs/{q(run_id)}"


def dashboard_test_url(run_id: str, test: str, query_index: int | str | None = None, metric: str | None = None) -> str:
    params = {}
    if query_index is not None:
        params["query_index"] = query_index
    if metric:
        params["metrics"] = metric
    search = ("?" + urllib.parse.urlencode(params)) if params else ""
    return f"https://performance.ci.clickhouse.com/runs/{q(run_id)}/tests/{q(test)}{search}"


def dashboard_query_url(run_id: str, test: str, query_index: int | str, metric: str | None = None) -> str:
    params = {}
    if metric:
        params["metrics"] = metric
    search = ("?" + urllib.parse.urlencode(params)) if params else ""
    return f"https://performance.ci.clickhouse.com/runs/{q(run_id)}/tests/{q(test)}/queries/{query_index}{search}"


def ascii_diff_chart(points: list[tuple[str, str, dict[str, Any] | None]]) -> str:
    numeric = []
    for time_text, run_id, row in points:
        if not row:
            continue
        try:
            numeric.append((time_text, run_id, float(row.get("diffPercent")), over_threshold(row)))
        except Exception:
            continue
    if not numeric:
        return "No numeric changed rows for chart."
    max_abs = max(abs(v) for _, _, v, _ in numeric) or 1.0
    lines = ["Diff chart, oldest → newest (`*` = over threshold):"]
    for time_text, run_id, diff, over in numeric:
        width = max(1, int(abs(diff) / max_abs * 24))
        bar = ("+" if diff >= 0 else "-") * width
        star = "*" if over == "yes" else " "
        lines.append(f"{time_text[:10]} {change_pct(diff):>9} {star} {bar} {run_id[:18]}…")
    return "\n".join(lines)

def leaf(stack: str) -> str:
    if not stack:
        return "—"
    return stack.split(";")[-1][:160]


def cmd_query(args: argparse.Namespace) -> None:
    params = {"metrics": args.metrics}
    path = f"/runs/{q(args.run_id)}/tests/{q(args.test)}/queries/{args.query_index}"
    data = fetch_json(args.base, path, params)
    print(f"API: {api_url(args.base, path, params)}\n")
    print(f"Dashboard: {dashboard_query_url(args.run_id, args.test, args.query_index, args.metric)}\n")
    print(f"# `{args.test}/{args.query_index}`\n")
    print(f"Query display: {data.get('queryDisplayName') or '—'}\n")
    if data.get("queryText"):
        print("```sql")
        print(data["queryText"].strip())
        print("```\n")
    rows = data.get("metrics") or []
    if args.metric:
        rows = [r for r in rows if r.get("metric") == args.metric]
    if args.arch:
        rows = [r for r in rows if r.get("arch") == args.arch]
    print_rows("Matching metric rows", rows, None, run_id=args.run_id)

    if args.with_confidence:
        conf_path = f"/runs/{q(args.run_id)}/tests/{q(args.test)}/confidence"
        conf, err = optional_fetch_json(args.base, conf_path, {"metrics": args.metrics})
        print(f"\n## Confidence\n\nAPI: {api_url(args.base, conf_path, {'metrics': args.metrics})}\n")
        if err:
            print(f"Not available: `{err}`\n")
        else:
            conf_rows = (conf.get("changedMetrics") or []) + (conf.get("unstableMetrics") or [])
            conf_rows = [r for r in conf_rows if r.get("queryIndex") == args.query_index]
            if args.metric:
                conf_rows = [r for r in conf_rows if r.get("metric") == args.metric]
            if args.arch:
                conf_rows = [r for r in conf_rows if r.get("arch") == args.arch]
            print_rows("Matching confidence rows", conf_rows, None, run_id=args.run_id)
            print_confidence_details(conf_rows)

    if args.with_trend:
        trend_path = f"/runs/{q(args.run_id)}/tests/{q(args.test)}/trend"
        trend_params = {"metric": args.metric, "queryIndex": args.query_index, "arch": args.arch}
        trend, err = optional_fetch_json(args.base, trend_path, trend_params)
        print(f"\n## Trend\n\nAPI: {api_url(args.base, trend_path, trend_params)}\n")
        if err:
            print(f"Not available: `{err}`")
        else:
            trend_points = trend.get("points") or []
            print(summarize_points(trend_points))
            ctx = selected_value_context(rows, trend_points, "trend distribution")
            if ctx:
                print(ctx)
            selected = trend.get("selectedRunPoint")
            if selected:
                print(f"Selected run marker: `{selected.get('time')}` sha={str(selected.get('label') or '')[:12]} (marker only; measured values are in the table above)")

    if args.with_history:
        hist_path = f"/history/{q(args.test)}/{args.query_index}"
        hist_params = {"metric": args.metric, "arch": args.arch}
        hist, err = optional_fetch_json(args.base, hist_path, hist_params)
        print(f"\n## History\n\nAPI: {api_url(args.base, hist_path, hist_params)}\n")
        if err:
            print(f"Not available: `{err}`")
        else:
            center = hist.get("centerTrend") or []
            print("Center trend:", summarize_points(center))
            ctx = selected_value_context(rows, center, "history center trend")
            if ctx:
                print(ctx)
            print(change_points_summary(hist.get("changePoints") or []))
            period = hist.get("periodComparisons") or []
            if period:
                print("\nPeriod comparisons returned by API:")
                for pc in period[:8]:
                    print(f"- {pc.get('direction','—')} magnitude={num(pc.get('magnitude'))} disparity={num(pc.get('disparity'))} leadingSha={str(pc.get('leadingSha') or '')[:12]} trust={pc.get('trustLabel') or '—'}")

    if args.with_coverage:
        cov_path = f"/runs/{q(args.run_id)}/tests/{q(args.test)}/coverage"
        cov_params = {"fileSet": "intersecting"}
        cov, err = optional_fetch_json(args.base, cov_path, cov_params)
        print(f"\n## Coverage / PR overlap\n\nAPI: {api_url(args.base, cov_path, cov_params)}\n")
        if err:
            print(f"Not available: `{err}`")
        else:
            totals = cov.get("totals") or {}
            files = cov.get("files") or []
            intersecting = totals.get('intersectingFiles')
            if intersecting is None:
                intersecting = len(files)
            print(f"coverageAvailable={cov.get('coverageAvailable')} source={cov.get('coverageSource') or '—'} message={cov.get('message') or '—'}")
            print(f"touched={totals.get('touchedFiles','—')} covered={totals.get('coveredFiles','—')} intersecting={intersecting}")
            for f in files[:20]:
                ranges = f.get("coverageRanges") or []
                print(f"- `{f.get('path')}` status={f.get('status')} changes={f.get('changes')} ranges={len(ranges)}")

    if args.with_flamegraph_diff:
        fg_path = f"/runs/{q(args.run_id)}/tests/{q(args.test)}/queries/{args.query_index}/flamegraph-diff"
        fg_params = {"metric": args.metric, "arch": args.arch, "traceType": args.trace_type}
        fg, err = optional_fetch_json(args.base, fg_path, fg_params)
        fg_url = api_url(args.base, fg_path, fg_params)
        ui_url = dashboard_query_url(args.run_id, args.test, args.query_index, args.metric)
        print(f"\n## Flamegraph diff ({args.trace_type})\n\nUI (query page, Flamegraphs card): {ui_url}\n")
        print(f"Raw API: {fg_url}\n")
        if err:
            print(f"Not available: `{err}`")
        else:
            frames = fg.get("frames") or []
            frames = sorted(frames, key=lambda f: abs(float(f.get("candidateSamples") or 0) - float(f.get("baselineSamples") or 0)), reverse=True)
            if not frames:
                print("No diff frames returned.")
            else:
                print("| Leaf | Baseline | Candidate | Delta |")
                print("|---|---:|---:|---:|")
                for f in frames[:20]:
                    b = float(f.get("baselineSamples") or 0)
                    c = float(f.get("candidateSamples") or 0)
                    print(f"| `{leaf(f.get('stack') or '')}` | {b:.0f} | {c:.0f} | {c-b:+.0f} |")



def fetch_pr_runs(base: str, pr: str, metrics: str) -> list[dict[str, Any]]:
    runs = fetch_json(base, "/runs", {"q": pr, "metrics": metrics}).get("items") or []
    try:
        pr_int = int(pr)
        runs = [x for x in runs if (x.get("identity") or {}).get("prNumber") == pr_int]
    except Exception:
        pass
    return runs


def collect_run_rows(base: str, items: list[dict[str, Any]], metrics: str, metric_filter: str = "client_time", arch_filter: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items:
        ident = item.get("identity") or {}
        run_id = ident.get("runId")
        if not run_id:
            continue
        detail = fetch_json(base, f"/runs/{q(run_id)}", {"metrics": metrics})
        for bucket, label in (("slowdowns", "slowdown"), ("speedups", "speedup"), ("unstableQueries", "high-noise/unstable-threshold")):
            for r in detail.get(bucket) or []:
                if metric_filter and r.get("metric") != metric_filter:
                    continue
                if arch_filter and r.get("arch") != arch_filter:
                    continue
                row = dict(r)
                row["runId"] = run_id
                row["runTime"] = ident.get("runTime")
                row["oldSha"] = ident.get("oldSha")
                row["newSha"] = ident.get("newSha")
                row["bucket"] = label
                # unstableQueries still carry a real direction; keep it for sorting/counts.
                row["direction"] = row.get("direction") or label
                rows.append(row)
    return rows


def attach_confidence_to_rows(base: str, metrics: str, rows: list[dict[str, Any]]) -> list[str]:
    """Attach dashboard confidence to API inventory rows in-place, grouped by run/test."""
    by_run_test: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        run_id = r.get("runId")
        test = r.get("test")
        if run_id and test:
            by_run_test[(run_id, test)].append(r)
    errors: list[str] = []
    for (run_id, test), group in sorted(by_run_test.items()):
        conf_path = f"/runs/{q(run_id)}/tests/{q(test)}/confidence"
        conf, err = optional_fetch_json(base, conf_path, {"metrics": metrics})
        if err:
            errors.append(f"{run_id} {test}: {err}")
            continue
        confidence_by_key: dict[tuple, dict[str, Any]] = {}
        for cr in (conf.get("changedMetrics") or []) + (conf.get("unstableMetrics") or []):
            if cr.get("confidence"):
                confidence_by_key[row_key(cr)] = cr.get("confidence")
        for r in group:
            row_conf = confidence_by_key.get(row_key(r))
            if row_conf:
                r["confidence"] = row_conf
    return errors


def tsv_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def float_or(value: Any, default: float) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


RAW_ALL_QUERY_METRICS_COLUMNS = [
    "metric_name",
    "left",
    "right",
    "diff",
    "times_change",
    "stat_threshold",
    "test",
    "query_index",
    "query_display_name",
    "changed_threshold",
    "unstable_threshold",
]
DEFAULT_CHANGED_THRESHOLD = 0.15
DEFAULT_UNSTABLE_THRESHOLD = 0.25


def read_text_maybe_compressed(path: str) -> str:
    """Read a file as text, transparently decompressing zstd/gzip content.

    CI stores text artifacts above a size threshold as zstd (see ci/praktika/s3.py), so a saved
    all-query-metrics.tsv may be either plain or compressed. Detected by magic bytes.
    """
    with open(path, "rb") as f:
        data = f.read()
    if data[:4] == b"\x28\xb5\x2f\xfd":  # zstd magic
        try:
            import zstandard  # optional dependency
            data = zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)).read()
        except ImportError:
            proc = subprocess.run(["zstd", "-dcq"], input=data, capture_output=True, timeout=120)
            if proc.returncode != 0:
                raise RuntimeError(f"zstd decompression failed: {proc.stderr.decode('utf-8', 'replace')}")
            data = proc.stdout
    elif data[:2] == b"\x1f\x8b":  # gzip magic
        data = gzip.decompress(data)
    return data.decode("utf-8")


def iter_tsv_dicts(path: str) -> list[dict[str, str]]:
    """Read named TSV or headerless raw all-query-metrics.tsv rows."""
    reader = csv.reader(io.StringIO(read_text_maybe_compressed(path)), delimiter="\t")
    raw_rows = [row for row in reader if row]
    if not raw_rows:
        return []
    first = raw_rows[0]
    header_names = {"metric", "metric_name", "test", "query_index", "queryIndex", "diff"}
    if header_names.intersection(first):
        header = first
        data_rows = raw_rows[1:]
    else:
        header = RAW_ALL_QUERY_METRICS_COLUMNS
        data_rows = raw_rows
    out: list[dict[str, str]] = []
    for row in data_rows:
        padded = row + [""] * max(0, len(header) - len(row))
        out.append(dict(zip(header, padded)))
    return out


def parse_perf_tsv(paths: list[str], metric_filter: str = "client_time", arch_filter: str = "") -> list[dict[str, Any]]:
    """Parse fetch_perf_report.py --tsv output or raw all-query-metrics.tsv-like files."""
    rows: list[dict[str, Any]] = []
    for path in paths:
        for src in iter_tsv_dicts(path):
            metric = src.get("metric") or src.get("metric_name") or metric_filter
            if metric_filter and metric != metric_filter:
                continue
            arch = src.get("arch") or arch_filter or ""
            if arch_filter and arch != arch_filter:
                continue
            try:
                diff = float(src.get("diff") or 0)
                threshold = float(src.get("stat_threshold") or src.get("threshold") or 0)
            except Exception:
                continue
            changed_threshold = float_or(src.get("changed_threshold"), DEFAULT_CHANGED_THRESHOLD)
            unstable_threshold = float_or(src.get("unstable_threshold"), DEFAULT_UNSTABLE_THRESHOLD)
            if "is_changed" in src:
                is_changed = tsv_bool(src.get("is_changed"))
            else:
                # Match ci/jobs/scripts/perf/compare.sh changed_fail:
                # abs(diff) > changed_threshold AND abs(diff) >= stat_threshold
                is_changed = abs(diff) > changed_threshold and abs(diff) >= threshold
            if "is_unstable" in src:
                is_unstable = tsv_bool(src.get("is_unstable"))
            else:
                # Match compare.sh unstable_fail:
                # NOT changed_fail AND stat_threshold > unstable_threshold
                is_unstable = (not is_changed) and threshold > unstable_threshold
            direction_raw = (src.get("direction") or "").lower()
            if direction_raw in {"slower", "slowdown"}:
                direction = "slowdown"
            elif direction_raw in {"faster", "speedup"}:
                direction = "speedup"
            elif diff > 0:
                direction = "slowdown"
            elif diff < 0:
                direction = "speedup"
            else:
                direction = "same"
            try:
                query_index: int | str = int(src.get("query_index") or src.get("queryIndex") or 0)
            except Exception:
                query_index = src.get("query_index") or src.get("queryIndex") or "—"
            rows.append({
                "source": path,
                "bucket": "changed" if is_changed else ("unstable" if is_unstable else "unchanged"),
                "metric": metric,
                "arch": arch,
                "shard": src.get("shard") or src.get("shard_num") or "",
                "test": src.get("test") or "",
                "queryIndex": query_index,
                "queryDisplayName": src.get("query") or src.get("query_display_name") or "",
                "oldValue": src.get("old") or src.get("left"),
                "newValue": src.get("new") or src.get("right"),
                "diffPercent": diff,
                "timesChange": src.get("times_change"),
                "statThreshold": threshold,
                "isChanged": is_changed,
                "isUnstable": is_unstable,
                "direction": direction,
            })
    return rows


def compare_key(row: dict[str, Any]) -> tuple:
    query_index = row.get("queryIndex")
    if query_index is None:
        query_index = -1
    return (row.get("arch") or "", row.get("test") or "", query_index, row.get("metric") or "")


def row_abs_diff(row: dict[str, Any]) -> float:
    try:
        return abs(float(row.get("diffPercent") or 0))
    except Exception:
        return 0.0


def row_evidence(row: dict[str, Any]) -> str:
    conf = confidence_status(row)
    if conf != "—":
        return conf
    if row.get("bucket") == "high-noise/unstable-threshold":
        if over_threshold(row) == "yes":
            return "**HIGH-NOISE** — raw diff is at/above threshold; confidence not returned"
        return "**HIGH-NOISE** — below adaptive/stat threshold; confidence not returned"
    return "**UNSCORED** — confidence not returned"


def print_inventory_table(rows: list[dict[str, Any]], limit: int | None = None) -> None:
    rows = sorted(rows, key=row_abs_diff, reverse=True)
    if limit:
        rows = rows[:limit]
    print("| Time | Direction | Arch | Test | Query | Dashboard | Old | New | Diff | Threshold | Over threshold | Evidence / next action |")
    print("|---|---|---|---|---:|---|---:|---:|---:|---:|---|---|")
    for r in rows:
        print(
            f"| {r.get('runTime','—')} | {r.get('direction','—')} | {r.get('arch','—')} | `{r.get('test','—')}` | {r.get('queryIndex','—')} | "
            f"{dashboard_test_query_link_for_row(r.get('runId'), r)} | {num(r.get('oldValue'))} | {num(r.get('newValue'))} | "
            f"{change_pct(r.get('diffPercent'))} | {change_pct(r.get('statThreshold'))} | {over_threshold(r)} | {row_evidence(r)} |"
        )
    print()


def print_inventory_rows(title: str, rows: list[dict[str, Any]], limit: int | None, note: str = "", group_evidence: bool = False) -> None:
    print(f"\n## {title}\n")
    if note:
        print(f"_{note}_\n")
    if not rows:
        print("No rows.\n")
        return
    if not group_evidence:
        print_inventory_table(rows, limit)
        return
    signal = [r for r in rows if not is_noise_or_uncertain(r)]
    rest = [r for r in rows if is_noise_or_uncertain(r)]
    if signal:
        print(f"### Signal evidence ({len(signal)})\n")
        print_inventory_table(signal, limit)
    if rest:
        print(f"### Noise / uncertain evidence ({len(rest)})\n")
        print_inventory_table(rest, limit)


def summary_rank(row: dict[str, Any]) -> tuple[int, float]:
    klass = evidence_class(row)
    weight = {
        "SLOWDOWN SIGNAL": 5,
        "STABLE SPEEDUP": 5,
        "SUSPECT": 3,
        "UNSCORED": 1,
        "NOISE": 0,
        "HIGH-NOISE": -1,
    }.get(klass, 0)
    return (weight, row_abs_diff(row))


def print_summary_bullets(title: str, rows: list[dict[str, Any]], limit: int = 5) -> None:
    print(f"\n### {title}\n")
    if not rows:
        print("- None in the current candidate.\n")
        return
    for r in sorted(rows, key=summary_rank, reverse=True)[:limit]:
        print(
            f"- {row_evidence(r)} — {r.get('direction','—')} `{r.get('arch','—')}` "
            f"`{r.get('test','—')}` q{r.get('queryIndex','—')} "
            f"{change_pct(r.get('diffPercent'))}; {dashboard_test_query_link_for_row(r.get('runId'), r)}"
        )
    print()


def print_inventory_summary(current_slow: list[dict[str, Any]], current_fast: list[dict[str, Any]], current_high_noise: list[dict[str, Any]]) -> None:
    likely_signal = [r for r in current_slow if not is_noise_or_uncertain(r)]
    likely_improvements = [r for r in current_fast if not is_noise_or_uncertain(r)]
    likely_noise = [r for r in current_slow + current_fast + current_high_noise if is_noise_or_uncertain(r)]
    print("\n## Summary layer\n")
    print("_Small generated shortlist before the full tables. Use this to decide what to read first; the full tables below remain the source of detail._")
    print_summary_bullets("Top likely signal", likely_signal)
    print_summary_bullets("Top likely noise", likely_noise)
    print_summary_bullets("Top improvements", likely_improvements)


def aggregate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        query_index = r.get("queryIndex")
        if query_index is None:
            query_index = -1
        by_key[(r.get("arch") or "", r.get("test") or "", query_index, r.get("metric") or "")].append(r)
    out = []
    for (arch, test, query_index, metric), vals in by_key.items():
        vals = sorted(vals, key=lambda r: r.get("runTime") or "")
        slow = [r for r in vals if r.get("direction") == "slowdown"]
        fast = [r for r in vals if r.get("direction") == "speedup"]
        high_noise = [r for r in vals if r.get("bucket") == "high-noise/unstable-threshold"]
        latest = vals[-1]
        max_slow = max([float(r.get("diffPercent") or 0) for r in slow], default=0.0)
        max_fast = min([float(r.get("diffPercent") or 0) for r in fast], default=0.0)
        out.append({
            "arch": arch,
            "test": test,
            "queryIndex": query_index,
            "metric": metric,
            "runs": len(vals),
            "slowdowns": len(slow),
            "speedups": len(fast),
            "max_slowdown": max_slow,
            "max_speedup": max_fast,
            "high_noise": len(high_noise),
            "flip": bool(slow and fast),
            "latest": latest,
            "max_abs": max(row_abs_diff(r) for r in vals),
        })
    return sorted(out, key=lambda x: (x["flip"], x["runs"], x["max_abs"]), reverse=True)


def print_aggregate_rows(title: str, aggs: list[dict[str, Any]], limit: int | None) -> None:
    print(f"\n## {title}\n")
    if not aggs:
        print("No rows.\n")
        return
    if limit:
        aggs = aggs[:limit]
    print("| Arch | Test | Query | Runs | Slowdowns | Speedups | High-noise rows | Direction flip | Max slowdown | Max speedup | Latest test page |")
    print("|---|---|---:|---:|---:|---:|---:|---|---:|---:|---|")
    for a in aggs:
        latest = a["latest"]
        print(
            f"| {a['arch'] or '—'} | `{a['test']}` | {a['queryIndex']} | {a['runs']} | {a['slowdowns']} | {a['speedups']} | {a['high_noise']} | "
            f"{'yes' if a['flip'] else 'no'} | {change_pct(a['max_slowdown']) if a['slowdowns'] else '—'} | "
            f"{change_pct(a['max_speedup']) if a['speedups'] else '—'} | {dashboard_test_query_link_for_row(latest.get('runId'), latest)} |"
        )
    print()


def print_tsv_rows(title: str, rows: list[dict[str, Any]], limit: int | None) -> None:
    print(f"\n## {title}\n")
    rows = sorted(rows, key=row_abs_diff, reverse=True)
    if limit:
        rows = rows[:limit]
    if not rows:
        print("No rows.\n")
        return
    print("| Bucket | Direction | Arch | Shard | Test | Query | Old | New | Diff | Times | Threshold | Changed | Unstable | Query text |")
    print("|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|")
    for r in rows:
        print(
            f"| {r.get('bucket','—')} | {r.get('direction','—')} | {r.get('arch','—')} | {r.get('shard','—')} | `{r.get('test','—')}` | {r.get('queryIndex','—')} | "
            f"{num(r.get('oldValue'))} | {num(r.get('newValue'))} | {change_pct(r.get('diffPercent'))} | {num(r.get('timesChange'))} | "
            f"{change_pct(r.get('statThreshold'))} | {'yes' if r.get('isChanged') else 'no'} | {'yes' if r.get('isUnstable') else 'no'} | {shorten(r.get('queryDisplayName'), 110)} |"
        )
    print()


def cmd_tsv_inventory(args: argparse.Namespace) -> None:
    rows = parse_perf_tsv(args.tsv, args.metric, args.arch)
    if not args.show_all:
        rows = [r for r in rows if r.get("isChanged") or r.get("isUnstable")]
    slow = [r for r in rows if r.get("isChanged") and r.get("direction") == "slowdown"]
    fast = [r for r in rows if r.get("isChanged") and r.get("direction") == "speedup"]
    unstable = [r for r in rows if r.get("isUnstable") and not r.get("isChanged")]
    print("# TSV/raw-artifact performance inventory\n")
    print("Input TSVs:")
    for path in args.tsv:
        print(f"- `{path}`")
    print("\n## Counts\n")
    print(f"- Rows considered: **{len(rows)}**")
    print(f"- Changed slowdowns: **{len(slow)}**")
    print(f"- Changed improvements/speedups: **{len(fast)}**")
    print(f"- Unstable/non-changed rows: **{len(unstable)}**")
    print_tsv_rows("Changed slowdowns from TSV/raw artifacts", slow, args.limit)
    print_tsv_rows("Changed improvements / speedups from TSV/raw artifacts", fast, args.limit)
    print_tsv_rows("Unstable/non-changed rows from TSV/raw artifacts", unstable, args.limit)



def cmd_pr_inventory(args: argparse.Namespace) -> None:
    items = fetch_pr_runs(args.base, args.pr, args.metrics)
    if not items:
        print(f"No runs found for PR {args.pr}.")
        return
    print(f"# PR {args.pr} performance inventory\n")
    print(f"API: {api_url(args.base, '/runs', {'q': args.pr, 'metrics': args.metrics})}\n")
    print("| Time | Run | Old | New | Arches | Changed | Slowdowns | Speedups |")
    print("|---|---|---|---|---|---:|---:|---:|")
    for item in items:
        ident = item.get("identity") or {}
        run_id = ident.get("runId", "")
        print(
            f"| {ident.get('runTime','—')} | [open]({dashboard_run_url(run_id)}) | `{str(ident.get('oldSha',''))[:12]}` | `{str(ident.get('newSha',''))[:12]}` | "
            f"{','.join(item.get('arches') or [])} | {item.get('changedQueries','—')} | {item.get('slowdownQueries','—')} | {item.get('speedupQueries','—')} |"
        )
    newest_sha = (items[0].get("identity") or {}).get("newSha")
    current_items = [it for it in items if (it.get("identity") or {}).get("newSha") == newest_sha]
    all_rows = collect_run_rows(args.base, items, args.metrics, args.metric, args.arch)
    confidence_errors: list[str] = []
    if getattr(args, "with_confidence", False):
        confidence_errors = attach_confidence_to_rows(args.base, args.metrics, all_rows)
    current_rows = [r for r in all_rows if r.get("runId") in {(it.get("identity") or {}).get("runId") for it in current_items}]
    current_slow = [r for r in current_rows if r.get("bucket") == "slowdown"]
    current_fast = [r for r in current_rows if r.get("bucket") == "speedup"]
    current_high_noise = [r for r in current_rows if r.get("bucket") == "high-noise/unstable-threshold"]
    all_slow = [r for r in all_rows if r.get("bucket") == "slowdown"]
    all_fast = [r for r in all_rows if r.get("bucket") == "speedup"]
    all_high_noise = [r for r in all_rows if r.get("bucket") == "high-noise/unstable-threshold"]
    print("\n## Counts\n")
    print(f"- Runs found: **{len(items)}**")
    print(f"- Current candidate SHA: `{str(newest_sha or '')[:12]}`; current run(s): **{len(current_items)}**")
    print(f"- Current candidate: changed slowdowns **{len(current_slow)}**, changed improvements/speedups **{len(current_fast)}**, high-noise/unstable-threshold rows **{len(current_high_noise)}**")
    print(f"- All PR runs: changed slowdowns **{len(all_slow)}**, changed improvements/speedups **{len(all_fast)}**, high-noise/unstable-threshold rows **{len(all_high_noise)}**")
    if getattr(args, "with_confidence", False):
        attached = sum(1 for r in all_rows if r.get("confidence"))
        print(f"- Confidence attached: **{attached}/{len(all_rows)}** rows")
        for err in confidence_errors[:5]:
            print(f"  - confidence unavailable for {err}")
    print_inventory_summary(current_slow, current_fast, current_high_noise)
    print_inventory_rows("Current changed slowdowns", current_slow, args.limit, "Raw changed rows with slowdown direction. Evidence can still downgrade a raw change to noise/flaky.", group_evidence=True)
    print_inventory_rows("Current changed improvements / speedups", current_fast, args.limit, "Raw changed rows with speedup direction. Evidence can still downgrade a raw change to noise/flaky.", group_evidence=True)
    print_inventory_rows("Current high-noise / unstable-threshold rows", current_high_noise, args.limit, "Separate from changed slowdowns/speedups. Inspect only if magnitude/context makes them relevant.")
    if not args.current_only and len(items) > 1:
        aggs = aggregate_rows(all_rows)
        flips = [a for a in aggs if a["flip"]]
        repeated = [a for a in aggs if a["runs"] > 1]
        if repeated:
            print_aggregate_rows("Rows repeated across PR runs", repeated, args.aggregate_limit)
        if flips:
            print_aggregate_rows("Direction flips across PR runs", flips, args.aggregate_limit)
        if args.show_all_run_tables:
            print_inventory_rows("All PR-run changed slowdowns", all_slow, args.limit)
            print_inventory_rows("All PR-run changed improvements / speedups", all_fast, args.limit)
            print_inventory_rows("All PR-run high-noise / unstable-threshold rows", all_high_noise, args.limit)


def master_test_name(row: dict[str, Any]) -> str:
    return f"{row.get('test')} #{row.get('queryIndex')}"


def fetch_master_check_counts(rows: list[dict[str, Any]], days: int, play_url: str, play_user: str) -> dict[tuple[str, str], dict[str, Any]]:
    by_arch: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        arch = r.get("arch") or ""
        test = r.get("test")
        query_index = r.get("queryIndex")
        if arch in {"amd", "arm"} and test is not None and query_index is not None:
            by_arch[arch].add(master_test_name(r))
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for arch, tests in sorted(by_arch.items()):
        if not tests:
            continue
        names = ",\n    ".join(sql_quote(t + "::new") for t in sorted(tests))
        sql = f"""
SELECT
    replaceRegexpOne(test_name, '::(new|old)$', '') AS test,
    countIf(test_status = 'slower') AS slower_count,
    countIf(test_status = 'faster') AS faster_count,
    countIf(test_status = 'unstable') AS unstable_count,
    count() AS total_runs
FROM default.checks
WHERE pull_request_number = 0
  AND check_name LIKE '%Performance%{arch}%'
  AND check_start_time >= now() - INTERVAL {int(days)} DAY
  AND test_name IN (
    {names}
  )
GROUP BY test
FORMAT JSONEachRow
"""
        for row in clickhouse_play_json_each_row(sql, play_url=play_url, user=play_user):
            out[(arch, row.get("test") or "")] = row
    return out


def master_counts_text(hist: dict[str, Any] | None) -> str:
    if not hist:
        return "0/0/0/0"
    return f"{hist.get('slower_count',0)}/{hist.get('faster_count',0)}/{hist.get('unstable_count',0)}/{hist.get('total_runs',0)}"


def classify_with_master_counts(row: dict[str, Any], hist: dict[str, Any] | None) -> str:
    if not hist:
        return "no master data"
    slower = int(hist.get("slower_count") or 0)
    faster = int(hist.get("faster_count") or 0)
    unstable = int(hist.get("unstable_count") or 0)
    total = int(hist.get("total_runs") or 0)
    if total <= 0:
        return "no master data"
    slower_rate = slower / total
    faster_rate = faster / total
    unstable_rate = unstable / total
    direction = row.get("direction")
    if unstable >= 5 or unstable_rate > 0.01:
        return "unstable on master"
    if direction == "slowdown":
        if slower_rate > 0.01 or slower >= 3:
            return "flaky/slower on master"
        if 1 <= slower <= 2:
            return "rarely slower on master"
        if total >= 50:
            return "new in PR — investigate"
        return "insufficient master history"
    if direction == "speedup":
        if slower_rate > 0.01 or slower >= 3:
            return "fixes known master regression"
        if faster_rate > 0.01 or faster >= 3:
            return "flaky/faster on master"
        if 1 <= faster <= 2:
            return "rarely faster on master"
        if total >= 50:
            return "new in PR — improvement"
        return "insufficient master history"
    return "unknown direction"


def cmd_master_checks(args: argparse.Namespace) -> None:
    if not args.tsv and not args.pr:
        raise SystemExit("master-checks requires either --pr or --tsv")
    rows: list[dict[str, Any]] = []
    source_note = ""
    if args.tsv:
        rows = [r for r in parse_perf_tsv(args.tsv, args.metric, args.arch) if r.get("isChanged")]
        source_note = "TSV/raw artifact rows"
    else:
        items = fetch_pr_runs(args.base, args.pr, args.metrics)
        if not args.all_runs:
            newest_sha = (items[0].get("identity") or {}).get("newSha") if items else None
            items = [it for it in items if (it.get("identity") or {}).get("newSha") == newest_sha]
        rows = collect_run_rows(args.base, items, args.metrics, args.metric, args.arch)
        rows = [r for r in rows if r.get("bucket") in {"slowdown", "speedup"}]
        source_note = "performance.ci API changed rows"
    if not rows:
        print("No changed rows found for master-check classification.")
        return
    # Attach dashboard confidence by default when rows came from performance.ci.
    # This lets the report separate likely signal from noise/uncertain rows.
    confidence_errors: list[str] = []
    if not args.tsv:
        confidence_errors = attach_confidence_to_rows(args.base, args.metrics, rows)
    counts = fetch_master_check_counts(rows, args.days, args.play_url, args.play_user)
    rows = rows_for_display(rows, args.limit)
    grouped: dict[tuple[str, str, str, str], list[tuple[dict[str, Any], dict[str, Any] | None]]] = defaultdict(list)
    for r in rows:
        key = (r.get("arch") or "", master_test_name(r))
        hist = counts.get(key)
        verdict = classify_with_master_counts(r, hist)
        signal_group = "noise / uncertain evidence" if is_noise_or_uncertain(r) else "not-noise evidence"
        grouped[(signal_group, r.get("arch") or "—", r.get("direction") or "—", verdict)].append((r, hist))

    verdict_order = {
        "new in PR — investigate": 0,
        "new in PR — improvement": 0,
        "fixes known master regression": 1,
        "rarely slower on master": 2,
        "rarely faster on master": 2,
        "flaky/slower on master": 3,
        "flaky/faster on master": 3,
        "unstable on master": 4,
        "insufficient master history": 5,
        "no master data": 6,
    }
    signal_order = {"not-noise evidence": 0, "noise / uncertain evidence": 1}
    direction_order = {"slowdown": 0, "speedup": 1}

    print(f"# Master-check classification ({args.days}d)\n")
    print(f"Source: {source_note}\n")
    print("Counts column is `master slower/faster/unstable/total` from `play.clickhouse.com default.checks` over master-only runs.\n")
    if confidence_errors:
        print(f"Confidence unavailable for {len(confidence_errors)} test/run groups; those rows may appear under noise / uncertain evidence.\n")

    current_signal_group = None
    for (signal_group, arch, direction, verdict), group in sorted(grouped.items(), key=lambda kv: (signal_order.get(kv[0][0], 9), kv[0][1], direction_order.get(kv[0][2], 9), verdict_order.get(kv[0][3], 9), -len(kv[1]))):
        if signal_group != current_signal_group:
            print(f"\n## {signal_group} ({sum(len(v) for k, v in grouped.items() if k[0] == signal_group)})\n")
            current_signal_group = signal_group
        print(f"\n### {arch} {direction} — {verdict} ({len(group)})\n")
        print("| Test | Query | Diff | Test page | Master s/f/u/total | Evidence |")
        print("|---|---:|---:|---|---:|---|")
        for r, hist in sorted(group, key=lambda item: row_abs_diff(item[0]), reverse=True):
            print(
                f"| `{r.get('test','—')}` | {r.get('queryIndex','—')} | {change_pct(r.get('diffPercent'))} | "
                f"{dashboard_test_query_link_for_row(r.get('runId'), r)} | {master_counts_text(hist)} | {row_evidence(r)} |"
            )


def cmd_pr_query_history(args: argparse.Namespace) -> None:
    runs = fetch_json(args.base, "/runs", {"q": args.pr, "metrics": args.metrics}).get("items") or []
    try:
        pr_int = int(args.pr)
        runs = [x for x in runs if (x.get("identity") or {}).get("prNumber") == pr_int]
    except Exception:
        pass
    print(f"# PR {args.pr}: `{args.test}/{args.query_index}` `{args.metric}/{args.arch}` across dashboard runs\n")
    print("| Time | Dashboard | Old | New | Diff | Threshold | Over threshold | Direction | Severity |")
    print("|---|---|---:|---:|---:|---:|---|---|---|")
    seen = 0
    chart_points: list[tuple[str, str, dict[str, Any] | None]] = []
    for item in runs:
        ident = item.get("identity") or {}
        run_id = ident.get("runId")
        if not run_id:
            continue
        try:
            detail = fetch_json(args.base, f"/runs/{q(run_id)}/tests/{q(args.test)}/queries/{args.query_index}", {"metrics": args.metrics})
        except Exception:
            dash = dashboard_query_url(run_id, args.test, args.query_index, args.metric)
            print(f"| {ident.get('runTime','—')} | [open]({dash}) | — | — | — | — | — | not available | — |")
            chart_points.append((ident.get('runTime','—'), run_id, None))
            continue
        rows = detail.get("metrics") or []
        match = [r for r in rows if r.get("metric") == args.metric and (not args.arch or r.get("arch") == args.arch)]
        dash = dashboard_query_url(run_id, args.test, args.query_index, args.metric)
        if not match:
            print(f"| {ident.get('runTime','—')} | [open]({dash}) | — | — | — | — | — | not listed | — |")
            chart_points.append((ident.get('runTime','—'), run_id, None))
            continue
        for r in match:
            seen += 1
            chart_points.append((ident.get('runTime','—'), run_id, r))
            print(
                f"| {ident.get('runTime','—')} | [open]({dash}) | {num(r.get('oldValue'))} | {num(r.get('newValue'))} | "
                f"{change_pct(r.get('diffPercent'))} | {change_pct(r.get('statThreshold'))} | {over_threshold(r)} | {r.get('direction','—')} | {r.get('severity','—')} |"
            )
    print(f"\nMatched rows: {seen}/{len(runs)} runs.\n")
    chronological = list(reversed(chart_points))
    print("```text")
    print(ascii_diff_chart(chronological))
    print("```\n")



def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--base", default=BASE_DEFAULT)
    p.add_argument("--metrics", default=DEFAULT_METRICS)
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("runs", help="List performance.ci runs for a PR/search query")
    sp.add_argument("--pr")
    sp.add_argument("--q")
    sp.set_defaults(func=cmd_runs)

    sp = sub.add_parser("run", help="Summarize one run")
    sp.add_argument("--run-id", required=True)
    sp.add_argument("--limit", type=int)
    sp.add_argument("--with-confidence", action="store_true")
    sp.set_defaults(func=cmd_run)

    sp = sub.add_parser("changes", help="Alias for run summary")
    sp.add_argument("--run-id", required=True)
    sp.add_argument("--limit", type=int)
    sp.add_argument("--with-confidence", action="store_true")
    sp.set_defaults(func=cmd_changes)

    sp = sub.add_parser("query", help="Inspect one run/test/query")
    sp.add_argument("--run-id", required=True)
    sp.add_argument("--test", required=True)
    sp.add_argument("--query-index", required=True, type=int)
    sp.add_argument("--metric", default="client_time")
    sp.add_argument("--arch", default="")
    sp.add_argument("--with-confidence", action="store_true")
    sp.add_argument("--with-trend", action="store_true")
    sp.add_argument("--with-history", action="store_true")
    sp.add_argument("--with-coverage", action="store_true")
    sp.add_argument("--with-flamegraph-diff", action="store_true")
    sp.add_argument("--trace-type", default="CPU", choices=["CPU", "REAL_TIME", "MEMORY"])
    sp.set_defaults(func=cmd_query)

    sp = sub.add_parser("pr-inventory", help="Inventory slowdowns and improvements for all performance.ci runs of a PR")
    sp.add_argument("--pr", required=True)
    sp.add_argument("--metric", default="client_time")
    sp.add_argument("--arch", default="")
    sp.add_argument("--limit", type=int, default=20)
    sp.add_argument("--aggregate-limit", type=int, default=20)
    sp.add_argument("--current-only", action="store_true")
    sp.add_argument("--show-all-run-tables", action="store_true", help="Also print full all-run top slowdown/speedup/high-noise tables")
    sp.add_argument("--with-confidence", action="store_true", help="Fetch per-test confidence for inventory rows")
    sp.set_defaults(func=cmd_pr_inventory)

    sp = sub.add_parser("tsv-inventory", help="Inventory rows from fetch_perf_report.py --tsv or raw all-query-metrics TSV files")
    sp.add_argument("--tsv", nargs="+", required=True)
    sp.add_argument("--metric", default="client_time")
    sp.add_argument("--arch", default="")
    sp.add_argument("--limit", type=int, default=20)
    sp.add_argument("--show-all", action="store_true")
    sp.set_defaults(func=cmd_tsv_inventory)

    sp = sub.add_parser("master-checks", help="Classify changed rows using 30-day master status counts from play.clickhouse.com default.checks")
    sp.add_argument("--pr", help="PR number; uses performance.ci API changed rows")
    sp.add_argument("--tsv", nargs="+", help="Optional fetch_perf_report.py --tsv / raw artifact TSV input instead of API rows")
    sp.add_argument("--metric", default="client_time")
    sp.add_argument("--arch", default="")
    sp.add_argument("--limit", type=int)
    sp.add_argument("--days", type=int, default=30)
    sp.add_argument("--all-runs", action="store_true", help="Use all PR dashboard runs instead of current candidate SHA only")
    sp.add_argument("--play-url", default=PLAY_DEFAULT)
    sp.add_argument("--play-user", default="explorer")
    sp.set_defaults(func=cmd_master_checks)

    sp = sub.add_parser("pr-query-history", help="Compare one query across all PR dashboard runs")
    sp.add_argument("--pr", required=True)
    sp.add_argument("--test", required=True)
    sp.add_argument("--query-index", required=True, type=int)
    sp.add_argument("--metric", default="client_time")
    sp.add_argument("--arch", default="")
    sp.set_defaults(func=cmd_pr_query_history)

    args = p.parse_args()
    if getattr(args, "pr", None) is None and getattr(args, "q", None) is None and args.cmd == "runs":
        p.error("runs requires --pr or --q")
    args.func(args)


if __name__ == "__main__":
    main()
