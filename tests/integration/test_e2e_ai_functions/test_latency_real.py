"""Suite B3: real-endpoint latency, reported rather than gated.

Absolute numbers for a pull-request description, and one cross-check that the architecture
metric from `test_latency_arch.py` survives contact with a real endpoint. Nothing here is
asserted unless `AI_E2E_LATENCY_GATE_REAL=1`, because a threshold on real-endpoint
wall-clock cannot survive model, region and load variation - which is the whole reason
Suite B measures shape against an injected delay instead.

Results are written as JSON with provenance and are deliberately **not** committed:
comparing a number from one session against another session is not meaningful.
"""

import math
import statistics
import time

import pytest

from . import corpus as ai_corpus
from .asserts import Report, current_sha, parse_json_rows
from .conftest import load_table, requires_live_endpoint

pytestmark = [pytest.mark.e2e, requires_live_endpoint]

CHAT = "map('credentials','ai_e2e_chat')"
EMBED = "map('credentials','ai_e2e_embed')"
CATEGORIES = "['positive','negative','neutral']"

WARMUP_CALLS = 2
SINGLES_BASE = 10
BATCH_BASE = 32


@pytest.fixture(scope="module")
def real_tables(instance, cfg):
    rows = ai_corpus.arith(cfg.data_scale)[: BATCH_BASE * cfg.data_scale]
    if len(rows) < BATCH_BASE * cfg.data_scale:
        rows = ai_corpus.arith(
            math.ceil(BATCH_BASE * cfg.data_scale / ai_corpus.ARITH_BASE)
        )[: BATCH_BASE * cfg.data_scale]
    load_table(
        instance,
        "b3_chat",
        [("id", "UInt32"), ("text", "String"), ("answer", "String")],
        rows,
    )
    load_table(
        instance,
        "b3_embed",
        [("id", "UInt32"), ("text", "String")],
        ai_corpus.embed_bulk(math.ceil(BATCH_BASE * cfg.data_scale / 40))[
            : BATCH_BASE * cfg.data_scale
        ],
    )
    yield
    for table in ("b3_chat", "b3_embed"):
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


def _single_expressions(cfg):
    """One expression per function, each over a single row."""
    row = ai_corpus.classify()[0]
    text = row["text"].replace("'", "\\'")
    extract_row = ai_corpus.extract()[0]
    return [
        ("aiGenerate", f"aiGenerate('What is 2 + 2? Reply with the number only.', {CHAT})"),
        ("aiClassify", f"aiClassify('{text}', {CATEGORIES}, {CHAT})"),
        (
            "aiExtract",
            f"aiExtract('{extract_row['text']}', '{ai_corpus.EXTRACT_SCHEMA}', {CHAT})",
        ),
        ("aiTranslate", f"aiTranslate('Order 12345 shipped today.', 'French', {CHAT})"),
        ("aiEmbed", f"length(aiEmbed('{text}', '{cfg.embed_model}', {EMBED}))"),
        (
            "aiSimilarity",
            f"aiSimilarity('{text}', '{text}', '{cfg.embed_model}', {EMBED})",
        ),
    ]


def _percentiles(values):
    if not values:
        return 0.0, 0.0
    ordered = sorted(values)
    p50 = statistics.median(ordered)
    p95 = ordered[min(len(ordered) - 1, int(0.95 * (len(ordered) - 1)))]
    return p50, p95


def test_b3_real_endpoint_latency(q, cfg, instance, real_tables):
    singles = SINGLES_BASE * cfg.data_scale
    batch_rows = BATCH_BASE * cfg.data_scale
    report = Report(
        "latency_real",
        {
            "target": cfg.target.name,
            "chat_model": cfg.chat_model,
            "embed_model": cfg.embed_model,
            "scale": cfg.data_scale,
            "git_sha": current_sha()[:12],
            "nproc": instance.exec_in_container(["nproc"]).strip(),
        },
    )

    # 1. Warm up, discarding the results: a cold connection or a cold endpoint would
    #    otherwise land in the first sample.
    for name, expression in _single_expressions(cfg):
        for attempt in range(WARMUP_CALLS):
            q.run(
                f"SELECT {expression} FROM numbers(1) FORMAT Null",
                case=f"b3_warmup_{name}_{attempt}",
                counting=True,
                rows=1,
            )

    # 2. Single-call latency per function.
    p50_by_function = {}
    for name, expression in _single_expressions(cfg):
        durations = []
        tokens = 0
        for attempt in range(singles):
            _result, events = q.run(
                f"SELECT {expression} FROM numbers(1) FORMAT Null",
                case=f"b3_single_{name}_{attempt}",
                counting=True,
                rows=1,
            )
            durations.append(events["query_duration_ms"])
            tokens += events["input_tokens"] + events["output_tokens"]
        p50, p95 = _percentiles(durations)
        p50_by_function[name] = p50
        report.add(
            f"single {name}",
            samples=singles,
            p50_ms=round(p50),
            p95_ms=round(p95),
            tokens=tokens,
        )

    # 3. Batch throughput. `WHERE NOT ignore(...)` forces per-row evaluation so no
    #    optimizer can prune the projection; the call count confirms nothing was skipped.
    batches = [
        ("aiGenerate", f"aiGenerate(text, {CHAT})", "b3_chat", batch_rows, "chat"),
        (
            "aiEmbed",
            f"aiEmbed(text, '{cfg.embed_model}', {EMBED})",
            "b3_embed",
            math.ceil(batch_rows / cfg.embed_batch_size),
            "embed",
        ),
    ]
    for name, expression, table, expected_calls, kind in batches:
        started = time.monotonic()
        _result, events = q.run(
            f"SELECT count() FROM {table} WHERE NOT ignore({expression}) FORMAT Null",
            case=f"b3_batch_{name}",
            counting=True,
            rows=batch_rows,
            timeout=3600,
        )
        wall_ms = (time.monotonic() - started) * 1000
        assert events["api_calls"] == expected_calls, (
            f"{name}: AIAPICalls={events['api_calls']}, expected {expected_calls} - "
            "the AI call was pruned or batched differently than assumed"
        )
        duration = events["query_duration_ms"]
        seconds_per_row = duration / 1000 / batch_rows
        tokens = events["input_tokens"] + events["output_tokens"]
        # 4. Effective concurrency with a measured D rather than an injected one. This is
        #    the number that says whether parallelism added in the implementation survives
        #    contact with the endpoint; a connection limit shows up as a plateau.
        concurrency = (
            round(batch_rows * p50_by_function[name] / duration, 2) if duration else 0.0
        )
        report.add(
            f"batch {name}",
            rows=batch_rows,
            api_calls=events["api_calls"],
            duration_ms=duration,
            wall_ms=round(wall_ms),
            s_per_row=round(seconds_per_row, 3),
            rows_per_s=round(batch_rows / (duration / 1000), 2) if duration else 0,
            tokens_per_s=round(tokens / (duration / 1000), 1) if duration else 0,
            effective_concurrency=concurrency,
        )

    # 5. Cost, priced by published rates when they are configured. Unpriced is reported as
    #    unpriced, never as a silent zero.
    total_in = sum(record.get("tokens", 0) for record in report.records)
    if cfg.price_in_per_1m or cfg.price_out_per_1m:
        usd = total_in * cfg.price_in_per_1m / 1_000_000
        report.add("cost", detail=f"~${usd:.4f} at the configured rates")
    else:
        report.add("cost", detail="unpriced (AI_E2E_PRICE_* unset)")

    path = report.flush()
    print(
        "\n"
        + report.render(
            [
                "case",
                "samples",
                "rows",
                "api_calls",
                "p50_ms",
                "p95_ms",
                "duration_ms",
                "s_per_row",
                "rows_per_s",
                "tokens_per_s",
                "effective_concurrency",
                "detail",
            ]
        )
    )
    print(f"[ai-e2e] wrote {path}")

    if not cfg.latency_gate_real:
        return

    # Gating real-endpoint latency needs a comparison point from the same session on the
    # same host; there is no committed number to check against, because a threshold on an
    # endpoint's wall-clock cannot survive model routing, region and load.
    if not cfg.compare_to:
        pytest.skip(
            "AI_E2E_LATENCY_GATE_REAL needs AI_E2E_COMPARE_TO pointing at a previous run"
        )
    table, regressions = report.compare(
        cfg.compare_to, columns=("s_per_row", "duration_ms")
    )
    print("\n" + table)
    assert not regressions, (
        "real-endpoint throughput regressed against the comparison run: "
        + ", ".join(f"{case} {field} {before} -> {after}" for case, field, before, after, _ in regressions)
    )
