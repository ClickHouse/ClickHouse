"""Shared assertions and readers for the AI-function end-to-end suite."""

import json
import math
import os
import uuid

# Reports land in the repository's `tmp/` unless redirected.
REPORT_DIR = os.environ.get("AI_E2E_REPORT_DIR") or os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", "tmp"
)

AI_SETTINGS = {"allow_experimental_ai_functions": 1}


def unique_query_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def read_ai_events(node, query_id):
    """AI ProfileEvents and duration for a finished query.

    `ProfileEvents[...]` yields 0 for an absent key, so `has_usage` gates on
    `mapContains`: a query that recorded no AI events at all is different from one that
    recorded zeros. Note that a query which threw records none of the five counters -
    they are incremented after the row loop - so cases expecting an exception must read
    the mock's /stats instead.
    """
    node.query("SYSTEM FLUSH LOGS")
    raw = node.query(
        f"""
        SELECT
            ProfileEvents['AIAPICalls'] AS api_calls,
            ProfileEvents['AIInputTokens'] AS input_tokens,
            ProfileEvents['AIOutputTokens'] AS output_tokens,
            ProfileEvents['AIRowsProcessed'] AS rows_processed,
            ProfileEvents['AIRowsSkipped'] AS rows_skipped,
            ProfileEvents['UserTimeMicroseconds'] AS user_time_us,
            mapContains(ProfileEvents, 'AIAPICalls') AS has_usage,
            query_duration_ms
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        FORMAT JSONEachRow
        """
    ).strip()
    assert raw, f"no QueryFinish row in system.query_log for query_id={query_id}"
    return json.loads(raw)


def expected_api_calls(kind, rows, batch=None):
    if kind == "chat":
        return rows
    if kind == "embed":
        assert batch, "embed expectations need the batch size"
        return math.ceil(rows / batch) if rows else 0
    raise ValueError(f"unknown kind {kind!r}")


def assert_ai_usage(
    events,
    kind,
    rows,
    batch=None,
    rows_processed=None,
    rows_skipped=0,
    reports_token_usage=True,
):
    """Assert the AI counters for one query.

    `api_calls` means one per row for the text functions and one per batch for the
    embedding functions, so the caller states which.
    """
    want_calls = expected_api_calls(kind, rows, batch)
    assert events["api_calls"] == want_calls, (
        f"AIAPICalls={events['api_calls']}, expected {want_calls} "
        f"(kind={kind}, rows={rows}, batch={batch})"
    )
    want_processed = rows if rows_processed is None else rows_processed
    assert events["rows_processed"] == want_processed, (
        f"AIRowsProcessed={events['rows_processed']}, expected {want_processed}"
    )
    assert events["rows_skipped"] == rows_skipped, (
        f"AIRowsSkipped={events['rows_skipped']}, expected {rows_skipped}"
    )
    if reports_token_usage and want_calls:
        assert events["input_tokens"] > 0, "AIInputTokens is 0 but the provider reports usage"


def budget_ms(cfg, rows, kind):
    """Wall-clock budget for a query, assuming today's serial architecture.

    Loose by design: if a change adds parallelism the budget stops being tight but never
    becomes wrong. Suite B owns tightness; this only catches pathological slowness.
    """
    if kind == "chat":
        per, units = cfg.per_call_budget_ms, rows
    else:
        per = cfg.embed_batch_budget_ms
        units = math.ceil(rows / cfg.embed_batch_size) if rows else 1
    return int(per * max(1, units) * 1.5) + 5000


def assert_within_budget(events, cfg, rows, kind, case):
    limit = budget_ms(cfg, rows, kind)
    assert events["query_duration_ms"] <= limit, (
        f"{case}: query took {events['query_duration_ms']} ms, budget {limit} ms"
    )
    return limit


def cosine(left, right):
    assert len(left) == len(right), f"dimension mismatch: {len(left)} vs {len(right)}"
    dot = sum(a * b for a, b in zip(left, right))
    norm_left = math.sqrt(sum(a * a for a in left))
    norm_right = math.sqrt(sum(b * b for b in right))
    assert norm_left > 0 and norm_right > 0, "zero-norm vector"
    return dot / (norm_left * norm_right)


def parse_json_rows(result):
    """Parse `FORMAT JSONEachRow` output.

    Model output can contain tabs and newlines, so TSV is not safe for these results.
    """
    return [json.loads(line) for line in result.splitlines() if line.strip()]


def parse_vector(text):
    """Parse one `Array(Float32)` cell as printed by the client."""
    text = text.strip()
    assert text.startswith("[") and text.endswith("]"), f"not an array: {text[:60]}"
    body = text[1:-1].strip()
    if not body:
        return []
    return [float(part) for part in body.split(",")]


class Report:
    """Machine-readable results plus a rendered markdown view.

    JSON is the persisted form and markdown is generated from it, so nothing ever parses
    a rendered table. Every record carries provenance, because a run nobody is watching
    still has to be interpretable afterwards.
    """

    def __init__(self, name, provenance):
        self.name = name
        self.provenance = dict(provenance)
        self.records = []
        self.path = os.path.join(REPORT_DIR, f"ai_e2e_{name}.json")

    def add(self, case, **fields):
        record = {"case": case}
        record.update(fields)
        self.records.append(record)
        return record

    def flush(self):
        os.makedirs(REPORT_DIR, exist_ok=True)
        payload = {
            "name": self.name,
            "provenance": self.provenance,
            "records": self.records,
        }
        try:
            with open(self.path, "w") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
        except OSError:
            # A report is a convenience; losing it must not fail a run.
            return None
        return self.path

    def compare(self, previous_path, columns=None):
        """A before/after table against a previous run's JSON.

        Host-dependent timings are never committed, so this is how they are judged: the
        same host, the same session, one binary against another - the same approach
        `ci/jobs/scripts/perf/compare.sh` takes for the performance tests.

        Returns (rendered_table, regressions) where regressions lists
        (case, field, before, after, ratio) for values that grew.
        """
        try:
            with open(previous_path) as handle:
                previous = json.load(handle)
        except (OSError, ValueError) as error:
            return f"(no comparison: {error})", []

        before_by_case = {record["case"]: record for record in previous.get("records", [])}
        lines = [
            f"## {self.name}: before/after ({os.path.basename(previous_path)} -> this run)",
            "| case | metric | before | after | change |",
            "|---|---|--:|--:|--:|",
        ]
        regressions = []
        for record in self.records:
            before = before_by_case.get(record["case"])
            if not before:
                continue
            for field, after_value in record.items():
                if field == "case" or field not in before:
                    continue
                before_value = before[field]
                if not isinstance(after_value, (int, float)) or isinstance(
                    after_value, bool
                ):
                    continue
                if not isinstance(before_value, (int, float)) or not before_value:
                    continue
                ratio = after_value / before_value
                lines.append(
                    f"| {record['case']} | {field} | {before_value} | {after_value} | "
                    f"{(ratio - 1) * 100:+.1f}% |"
                )
                if columns and field in columns and ratio > 1.0:
                    regressions.append(
                        (record["case"], field, before_value, after_value, ratio)
                    )
        return "\n".join(lines), regressions

    def render(self, columns):
        lines = [f"## {self.name} ({format_provenance(self.provenance)})"]
        lines.append("| " + " | ".join(columns) + " |")
        lines.append("|" + "|".join("---" for _ in columns) + "|")
        for record in self.records:
            cells = [str(record.get(column, "")) for column in columns]
            lines.append("| " + " | ".join(cells) + " |")
        return "\n".join(lines)


def format_provenance(provenance):
    return ", ".join(f"{key}={value}" for key, value in sorted(provenance.items()))
