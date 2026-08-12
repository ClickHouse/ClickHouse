"""Suite A3: concurrent use is correct.

Two properties: **isolation** - no result, counter or vector crosses a query boundary -
and **liveness** - nothing deadlocks or serializes unexpectedly.

Endpoint throttling is recorded, not asserted: if the gateway starts rate-limiting, that
must be visible as a report line rather than read as a ClickHouse bug. Correctness is
asserted unconditionally.
"""

import concurrent.futures
import math
import statistics

import pytest

from . import corpus as ai_corpus
from .asserts import (
    Report,
    cosine,
    parse_json_rows,
    read_ai_events,
    unique_query_id,
)
from .conftest import load_table, requires_capable_model, requires_live_endpoint

pytestmark = [pytest.mark.e2e, requires_live_endpoint]

CHAT = "map('credentials','ai_e2e_chat')"
EMBED = "map('credentials','ai_e2e_embed')"
CATEGORIES = "['positive','negative','neutral']"
QUERY_TIMEOUT = 600


@pytest.fixture(scope="module")
def concurrency(cfg):
    return min(4 * cfg.data_scale, 16)


@pytest.fixture(scope="module")
def a3_tables(instance, cfg):
    rows = ai_corpus.arith(cfg.data_scale)
    load_table(
        instance,
        "a3_arith",
        [("id", "UInt32"), ("text", "String"), ("answer", "String")],
        rows,
    )
    load_table(
        instance,
        "a3_embed",
        [("id", "UInt32"), ("text", "String")],
        ai_corpus.embed_bulk()[:16],
    )
    load_table(
        instance,
        "a3_classify",
        [("id", "UInt32"), ("text", "String"), ("label", "String")],
        ai_corpus.classify(),
    )
    # Eight parts, so several pipeline streams can call `executeImpl` at once.
    instance.query("DROP TABLE IF EXISTS a3_parts SYNC")
    instance.query(
        "CREATE TABLE a3_parts (id UInt32, text String, label String) "
        "ENGINE = MergeTree ORDER BY id"
    )
    for row in ai_corpus.classify():
        instance.query(
            "INSERT INTO a3_parts VALUES ({}, '{}', '{}')".format(
                row["id"], row["text"].replace("'", "\\'"), row["label"]
            )
        )
    yield
    for table in ("a3_arith", "a3_embed", "a3_classify", "a3_parts"):
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.fixture(scope="module")
def report(cfg, concurrency):
    report = Report(
        "suite_a3",
        {"target": cfg.target.name, "concurrency": concurrency, "scale": cfg.data_scale},
    )
    yield report
    report.flush()
    print("\n" + report.render(["case", "queries", "detail", "verdict"]))


def _run_concurrently(instance, q, jobs):
    """Run `jobs` (case, sql) concurrently, each on its own client connection.

    Returns {case: (result, error, query_id)}. `node.query` spawns a client subprocess per
    call and defaults to no timeout, so an explicit one is passed: otherwise a hung
    endpoint blocks until the harness kills pytest.
    """
    outcomes = {}

    def run(case, sql, settings):
        query_id = unique_query_id(case)
        try:
            result = instance.query(
                sql,
                settings=q.settings(settings),
                query_id=query_id,
                timeout=QUERY_TIMEOUT,
            )
            return case, result, None, query_id
        except Exception as error:
            return case, None, str(error), query_id

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures = [pool.submit(run, case, sql, settings) for case, sql, settings in jobs]
        for future in concurrent.futures.as_completed(futures):
            case, result, error, query_id = future.result()
            outcomes[case] = (result, error, query_id)
    return outcomes


# ---------------------------------------------------------------------------
# A3-1 same function, unique token per query
# ---------------------------------------------------------------------------


@requires_capable_model
def test_a3_1_no_cross_talk(instance, q, concurrency, a3_tables, report):
    jobs = [
        (
            f"a3_1_{index}",
            f"SELECT aiGenerate('Reply with exactly the token TOKQ{index} and nothing "
            f"else.', {CHAT}) AS out FROM numbers(1) FORMAT JSONEachRow",
            {},
        )
        for index in range(concurrency)
    ]
    outcomes = _run_concurrently(instance, q, jobs)

    failures = []
    for index in range(concurrency):
        result, error, _query_id = outcomes[f"a3_1_{index}"]
        assert error is None, f"query {index} failed: {error}"
        out = parse_json_rows(result)[0]["out"]
        own = f"TOKQ{index}"
        if own not in out:
            failures.append(f"query {index} lost its own token: {out[:60]!r}")
        foreign = [
            f"TOKQ{other}"
            for other in range(concurrency)
            if other != index and f"TOKQ{other}" in out
        ]
        if foreign:
            failures.append(f"query {index} saw {foreign}")
    report.add(
        "a3_1 cross-talk",
        queries=concurrency,
        detail="unique token per query",
        verdict="ok" if not failures else "; ".join(failures[:2]),
    )
    assert not failures, failures


# ---------------------------------------------------------------------------
# A3-2 different functions at once, counters attributed per query
# ---------------------------------------------------------------------------


def test_a3_2_mixed_functions(instance, q, cfg, a3_tables, report):
    model = cfg.embed_model
    jobs = [
        (
            "a3_2_generate",
            f"SELECT aiGenerate(text, {CHAT}) AS out FROM a3_arith LIMIT 2 "
            f"FORMAT JSONEachRow",
            {"max_block_size": 4096, "max_threads": 1},
        ),
        (
            "a3_2_classify",
            f"SELECT aiClassify(text, {CATEGORIES}, {CHAT}) AS out FROM a3_classify "
            f"LIMIT 2 FORMAT JSONEachRow",
            {"max_block_size": 4096, "max_threads": 1},
        ),
        (
            "a3_2_embed",
            f"SELECT aiEmbed(text, '{model}', {EMBED}) AS vec FROM a3_embed LIMIT 4 "
            f"FORMAT JSONEachRow",
            {"max_block_size": 4096, "max_threads": 1},
        ),
        (
            "a3_2_translate",
            f"SELECT aiTranslate('Order 99 shipped today.', 'French', {CHAT}) AS out "
            f"FROM numbers(1) FORMAT JSONEachRow",
            {},
        ),
    ]
    outcomes = _run_concurrently(instance, q, jobs)

    expected_calls = {
        "a3_2_generate": 2,
        "a3_2_classify": 2,
        "a3_2_embed": 1,  # four texts, one batch
        "a3_2_translate": 1,
    }
    mismatches = []
    for case, expected in expected_calls.items():
        result, error, query_id = outcomes[case]
        assert error is None, f"{case} failed: {error}"
        assert parse_json_rows(result), f"{case} returned nothing"
        events = read_ai_events(instance, query_id)
        if events["api_calls"] != expected:
            mismatches.append(f"{case}: {events['api_calls']} calls, expected {expected}")
    report.add(
        "a3_2 mixed functions",
        queries=len(jobs),
        detail="per-query ProfileEvents",
        verdict="ok" if not mismatches else "; ".join(mismatches),
    )
    assert not mismatches, (
        "counters are not attributed per query: " + "; ".join(mismatches)
    )


# ---------------------------------------------------------------------------
# A3-3 disjoint slices of the same table, row alignment preserved
# ---------------------------------------------------------------------------


@requires_capable_model
def test_a3_3_row_alignment(instance, q, cfg, concurrency, a3_tables, report):
    rows = ai_corpus.arith(cfg.data_scale)
    per_query = max(2, len(rows) // concurrency)
    jobs = []
    slices = {}
    for index in range(concurrency):
        start = index * per_query
        chunk = rows[start : start + per_query]
        if not chunk:
            continue
        ids = ",".join(str(row["id"]) for row in chunk)
        slices[f"a3_3_{index}"] = chunk
        jobs.append(
            (
                f"a3_3_{index}",
                f"SELECT id, answer, aiGenerate(text, {CHAT}) AS out FROM a3_arith "
                f"WHERE id IN ({ids}) ORDER BY id FORMAT JSONEachRow",
                {"max_block_size": 4096, "max_threads": 1},
            )
        )
    outcomes = _run_concurrently(instance, q, jobs)

    failures = []
    for case, chunk in slices.items():
        result, error, _query_id = outcomes[case]
        assert error is None, f"{case} failed: {error}"
        parsed = parse_json_rows(result)
        if len(parsed) != len(chunk):
            failures.append(f"{case}: {len(parsed)} rows, expected {len(chunk)}")
            continue
        expected_ids = [row["id"] for row in chunk]
        if [row["id"] for row in parsed] != expected_ids:
            failures.append(f"{case}: foreign rows appeared")
            continue
        for row in parsed:
            if row["answer"] not in row["out"]:
                failures.append(f"{case}: row {row['id']} got another row's answer")
                break
    report.add(
        "a3_3 row alignment",
        queries=len(jobs),
        detail=f"{per_query} rows per query",
        verdict="ok" if not failures else "; ".join(failures[:2]),
    )
    assert not failures, failures


# ---------------------------------------------------------------------------
# A3-4 concurrent embeddings match a serial reference
# ---------------------------------------------------------------------------


def test_a3_4_embeddings_match_reference(instance, q, cfg, concurrency, a3_tables, report):
    model = cfg.embed_model
    reference_sql = (
        f"SELECT id, aiEmbed(text, '{model}', {EMBED}) AS vec FROM a3_embed "
        f"ORDER BY id LIMIT 8 FORMAT JSONEachRow"
    )
    reference_result, _events = q.run(
        reference_sql, case="a3_4_reference", counting=True, rows=8
    )
    reference = {row["id"]: row["vec"] for row in parse_json_rows(reference_result)}

    jobs = [
        (f"a3_4_{index}", reference_sql, {"max_block_size": 4096, "max_threads": 1})
        for index in range(concurrency)
    ]
    outcomes = _run_concurrently(instance, q, jobs)

    worst = 0.0
    for case, (result, error, _query_id) in outcomes.items():
        assert error is None, f"{case} failed: {error}"
        for row in parse_json_rows(result):
            worst = max(worst, 1 - cosine(reference[row["id"]], row["vec"]))
    report.add(
        "a3_4 embedding stability",
        queries=len(jobs),
        detail=f"worst 1-cos vs serial reference = {worst:.2e}",
        verdict="ok" if worst <= 1e-4 else "vectors differ under load",
    )
    assert worst <= 1e-4, (
        f"concurrent embeddings differ from the serial reference (worst 1-cos {worst:.2e}); "
        "batch indices may be crossing between queries"
    )


# ---------------------------------------------------------------------------
# A3-5 quotas do not leak between queries
# ---------------------------------------------------------------------------


def test_a3_5_quota_is_per_query(instance, q, a3_tables, report):
    jobs = [
        (
            f"a3_5_{index}",
            f"SELECT aiGenerate(text, {CHAT}) AS out FROM a3_arith ORDER BY id LIMIT 3 "
            f"FORMAT JSONEachRow",
            {
                "ai_function_max_api_calls_per_query": 3,
                "ai_function_throw_on_quota_exceeded": 1,
                "max_block_size": 4096,
                "max_threads": 1,
            },
        )
        for index in range(2)
    ]
    outcomes = _run_concurrently(instance, q, jobs)

    details = []
    for case, (result, error, query_id) in outcomes.items():
        assert error is None, f"{case} hit the quota, so it is not per query: {error}"
        events = read_ai_events(instance, query_id)
        details.append(f"{case}: {events['api_calls']} calls")
        assert events["api_calls"] == 3, f"{case}: {events['api_calls']} calls"
        assert events["rows_skipped"] == 0, f"{case} skipped rows"
    report.add(
        "a3_5 quota isolation",
        queries=2,
        detail=", ".join(details),
        verdict="ok",
    )


# ---------------------------------------------------------------------------
# A3-6 intra-query parallelism: several streams, one correct result set
# ---------------------------------------------------------------------------


def test_a3_6_multi_stream_single_query(q, cfg, a3_tables, report):
    rows = ai_corpus.classify()
    durations = {}
    for threads in (1, 8):
        result, events = q.run(
            f"SELECT id, label, aiClassify(text, {CATEGORIES}, {CHAT}) AS out "
            f"FROM a3_parts ORDER BY id FORMAT JSONEachRow",
            case=f"a3_6_{threads}t",
            settings={"max_threads": threads, "max_block_size": 2},
        )
        parsed = parse_json_rows(result)
        assert len(parsed) == len(rows)
        assert events["api_calls"] == len(rows), (
            f"threads={threads}: AIAPICalls={events['api_calls']}, expected "
            f"{len(rows)} - duplicated or dropped calls under multi-stream execution"
        )
        assert events["rows_skipped"] == 0
        durations[threads] = events["query_duration_ms"]
        if not cfg.toy_model:
            wrong = [row for row in parsed if row["out"].strip() != row["label"]]
            assert not wrong, f"threads={threads}: wrong labels {wrong[:2]}"

    speedup = round(durations[1] / max(1, durations[8]), 2)
    report.add(
        "a3_6 multi-stream",
        queries=2,
        detail=f"1 thread {durations[1]} ms, 8 threads {durations[8]} ms",
        verdict=f"speedup {speedup}x",
    )
    print(
        f"\n[ai-e2e] intra-query parallelism: {durations[1]} ms at 1 thread vs "
        f"{durations[8]} ms at 8 threads (speedup {speedup}x)"
    )


# ---------------------------------------------------------------------------
# A3-7 endpoint health under concurrency (report-only)
# ---------------------------------------------------------------------------


def test_a3_7_endpoint_health(instance, q, cfg, concurrency, a3_tables, report):
    jobs = [
        (
            f"a3_7_{index}",
            f"SELECT aiGenerate('Reply with the word OK.', {CHAT}) AS out "
            f"FROM numbers(1) FORMAT JSONEachRow",
            {},
        )
        for index in range(concurrency)
    ]
    outcomes = _run_concurrently(instance, q, jobs)

    durations = []
    throttled = 0
    failed = 0
    for case, (_result, error, query_id) in outcomes.items():
        if error:
            failed += 1
            if "429" in error or "rate" in error.lower():
                throttled += 1
            continue
        durations.append(read_ai_events(instance, query_id)["query_duration_ms"])

    if durations:
        durations.sort()
        p50 = statistics.median(durations)
        p95 = durations[min(len(durations) - 1, int(0.95 * len(durations)))]
    else:
        p50 = p95 = 0
    report.add(
        "a3_7 endpoint health",
        queries=concurrency,
        detail=f"p50 {p50:.0f} ms / p95 {p95:.0f} ms",
        verdict=f"{throttled} throttled, {failed - throttled} other failures",
    )
    print(
        f"\n[ai-e2e] concurrency health: p50={p50:.0f}ms p95={p95:.0f}ms "
        f"throttled={throttled} failed={failed}"
    )
    # Deliberately no assertion on throttling: correctness is asserted by A3-1..A3-6, and
    # a rate-limiting endpoint is an endpoint fact, not a ClickHouse regression.
