"""Suite B, structural half: exact integers against the mock at `delay_ms = 0`.

These cases assert `AIAPICalls` and request counts, never wall-clock, so they are cheap,
deterministic, tolerant of a shared host, and safe to run in parallel with anything. The
timing cases live in `test_latency_arch.py`.

What the laziness table measures: how many AI calls a query shape issues today versus how
many an implementation that evaluated AI functions as lazily as possible would issue. An
optimizer change that makes AI functions lazy shows up as `current` values collapsing
toward `ideal`. See 02-latency.md section 6.
"""

import math

import pytest

from . import baselines
from . import corpus as ai_corpus
from .asserts import Report
from .conftest import MOCK_PORT, load_table

pytestmark = pytest.mark.e2e

CATEGORIES = "['positive','negative','neutral']"
CHAT = f"aiClassify(x, {CATEGORIES}, map('credentials','ai_e2e_mock_chat'))"
EMB = "aiEmbed(x, 'mock-model', map('credentials','ai_e2e_mock_embed'))"

# Laziness runs over one part with a small block, so per-block effects are visible.
LAZY_BLOCK = 64
LAZY_ROWS = 256
DISTINCT_VALUES = 4


def _blocks(rows, block=LAZY_BLOCK):
    return math.ceil(rows / block)


@pytest.fixture(scope="module")
def mock_tables(instance):
    """Tables used by the mock-driven cases."""
    load_table(
        instance, "struct_rows", [("id", "UInt32"), ("x", "String")], ai_corpus.mock_rows(32)
    )
    load_table(
        instance,
        "lazy_rows",
        [("id", "UInt32"), ("x", "String")],
        ai_corpus.mock_rows(LAZY_ROWS),
    )
    load_table(
        instance,
        "lazy_dup",
        [("id", "UInt32"), ("x", "String")],
        ai_corpus.mock_rows(LAZY_ROWS, distinct_values=DISTINCT_VALUES),
    )
    yield
    for table in ("struct_rows", "lazy_rows", "lazy_dup"):
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


# ---------------------------------------------------------------------------
# B2-2: one call per row, and the connection is reused across them.
# ---------------------------------------------------------------------------


def test_call_count_and_connection_reuse(q, clean_mock, mock_tables):
    _result, events = q.run(
        f"SELECT {CHAT} FROM struct_rows FORMAT Null",
        case="b2_2",
        counting=True,
        rows=32,
    )
    assert events["api_calls"] == 32, "one HTTP request per row"
    assert events["rows_processed"] == 32
    assert events["rows_skipped"] == 0

    stats = clean_mock.stats()
    assert stats["requests"] == 32, stats
    assert stats["connections"] >= 1
    # M5 is a ratio, not an absolute count: the HTTP session pool is server-wide, so a
    # warm pool can serve a whole query without opening anything new.
    ratio = stats["requests"] / stats["connections"]
    assert ratio >= 8, (
        f"requests/connections = {ratio:.1f}; a new connection per few requests means "
        "keep-alive stopped working, which is real latency against a real endpoint"
    )


# ---------------------------------------------------------------------------
# B2-3 (counts): embedding batches. The timing relation is asserted separately.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("batch", [1, 8, 32])
def test_embed_batch_counts(q, clean_mock, mock_tables, batch):
    _result, events = q.run(
        f"SELECT {EMB} FROM struct_rows FORMAT Null",
        case=f"b2_3_batch{batch}",
        counting=True,
        rows=32,
        settings={"ai_function_embedding_max_batch_size": batch},
    )
    expected = math.ceil(32 / batch)
    assert events["api_calls"] == expected, f"batch={batch}"
    assert events["rows_processed"] == 32
    assert clean_mock.stats()["requests"] == expected


# ---------------------------------------------------------------------------
# B2-10 / B2-11 (counts): a failing query records no ProfileEvents, so the mock's
# request count is the only way to see how many attempts were made.
# ---------------------------------------------------------------------------


def test_timeout_issues_one_request(q, clean_mock, mock_tables):
    clean_mock.configure(delay_ms=3000)
    error = q.error(
        f"SELECT {CHAT} FROM struct_rows LIMIT 1",
        case="b2_10",
        settings={
            "ai_function_request_timeout_sec": 1,
            "ai_function_max_retries": 0,
            "max_block_size": 1024,
            "max_threads": 1,
        },
    )
    assert error, "a request slower than the timeout must fail the query"
    stats = clean_mock.stats()
    assert stats["requests"] == 1, f"no retries were configured: {stats}"


def test_retries_issue_one_request_per_attempt(q, clean_mock, mock_tables):
    clean_mock.configure(delay_ms=3000)
    error = q.error(
        f"SELECT {CHAT} FROM struct_rows LIMIT 1",
        case="b2_11",
        settings={
            "ai_function_request_timeout_sec": 1,
            "ai_function_max_retries": 2,
            "ai_function_retry_initial_delay_ms": 100,
            "max_block_size": 1024,
            "max_threads": 1,
        },
    )
    assert error
    stats = clean_mock.stats()
    assert stats["requests"] == 3, f"expected 1 attempt + 2 retries: {stats}"


# ---------------------------------------------------------------------------
# Quota scope. The settings are named `..._per_query` and documented as bounding a
# query, but `AIQuotaTracker` is a stack local in `executeImpl`
# (`FunctionBaseAI.cpp:499`, `aiEmbed.cpp:135`, `aiSimilarity.cpp:162`) with no shared
# state, so each call gets a fresh allowance. This measures the real multiplier.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=False,
    reason="the `..._per_query` quotas are enforced per executeImpl call, so a query with "
    "several blocks or streams gets several allowances (measured: 3x on an 8-part table)",
)
def test_api_call_quota_scope(q, clean_mock, mock_tables, instance):
    """Is `ai_function_max_api_calls_per_query` really per query?

    `AIQuotaTracker` is a stack local in `executeImpl` (`FunctionBaseAI.cpp:499`) with no
    shared state, so a fresh allowance per call is the structural expectation - and a
    query is many calls: one per block, and blocks run concurrently across streams.

    A single configuration cannot tell "the quota is per query" from "the reader happened
    to produce one block", since both predict `calls == limit`. This probes several shapes
    whose block counts differ, and asserts on the shape where they must diverge.
    """
    limit = 10
    instance.query("DROP TABLE IF EXISTS quota_parts SYNC")
    instance.query(
        "CREATE TABLE quota_parts (id UInt32, x String) ENGINE = MergeTree ORDER BY id"
    )
    for part in range(8):
        values = ", ".join(
            f"({part * 32 + i}, 'quota row {part * 32 + i}')" for i in range(32)
        )
        instance.query(f"INSERT INTO quota_parts VALUES {values}")

    shapes = [
        ("1 part, block=64,  threads=1", "lazy_rows", 64, 1),
        ("1 part, block=1024,threads=1", "lazy_rows", 1024, 1),
        ("8 parts,block=64,  threads=1", "quota_parts", 64, 1),
        ("8 parts,block=8,   threads=8", "quota_parts", 8, 8),
    ]
    measured = {}
    for label, table, block, threads in shapes:
        clean_mock.reset()
        _result, events = q.run(
            f"SELECT {CHAT} FROM {table} FORMAT Null",
            case="quota_scope",
            settings={
                "max_block_size": block,
                "max_threads": threads,
                "preferred_block_size_bytes": 0,
                "ai_function_max_api_calls_per_query": limit,
                "ai_function_throw_on_quota_exceeded": 0,
            },
        )
        measured[label] = events["api_calls"]
        print(
            f"\n[ai-e2e] quota {label}: {events['api_calls']} calls "
            f"(limit {limit}), {events['rows_skipped']} skipped"
        )
    instance.query("DROP TABLE IF EXISTS quota_parts SYNC")

    # How many allowances does a *large* scan get? With one call per row and a limit far
    # below the row count, `calls / limit` is exactly the number of blocks the query
    # produced - which is the multiplier on the cost cap, and tells you which limits are
    # unreachable (any limit above the largest single block's row count).
    instance.query("DROP TABLE IF EXISTS quota_big SYNC")
    instance.query(
        "CREATE TABLE quota_big (id UInt32, x String) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(
        "INSERT INTO quota_big SELECT number, concat('row ', toString(number)) "
        "FROM numbers(20000)"
    )
    instance.query("OPTIMIZE TABLE quota_big FINAL")
    clean_mock.reset()
    big_limit = 100
    _result, events = q.run(
        f"SELECT {CHAT} FROM quota_big FORMAT Null",
        case="quota_blocks",
        settings={
            "ai_function_max_api_calls_per_query": big_limit,
            "ai_function_throw_on_quota_exceeded": 0,
        },
    )
    blocks_seen = events["api_calls"] / big_limit
    # And again with a block size below the row count, to show the count follows
    # `max_block_size` rather than the granule size.
    clean_mock.reset()
    _r2, e2 = q.run(
        f"SELECT {CHAT} FROM quota_big FORMAT Null",
        case="quota_blocks_small",
        settings={
            "max_block_size": 8192,
            "preferred_block_size_bytes": 0,
            "ai_function_max_api_calls_per_query": big_limit,
            "ai_function_throw_on_quota_exceeded": 0,
        },
    )
    print(
        f"\n[ai-e2e] same 20000 rows at max_block_size=8192: {e2['api_calls']} calls "
        f"-> {e2['api_calls'] / big_limit:.0f} blocks"
    )
    print(
        f"\n[ai-e2e] 20000 rows at default max_block_size: {events['api_calls']} calls "
        f"for a limit of {big_limit} -> {blocks_seen:.0f} blocks, so the effective cap is "
        f"{blocks_seen:.0f}x what was configured"
    )
    instance.query("DROP TABLE IF EXISTS quota_big SYNC")

    over = {label: calls for label, calls in measured.items() if calls > limit}
    assert not over, (
        f"`ai_function_max_api_calls_per_query` = {limit} but these shapes exceeded it: "
        f"{over}. The tracker is per `executeImpl` call, so every extra block or stream "
        "grants a fresh allowance and the cost cap scales with the data instead of "
        "bounding it."
    )


# ---------------------------------------------------------------------------
# B2-5 (count pass): the laziness table.
# ---------------------------------------------------------------------------


def _lazy_scenarios():
    n = LAZY_ROWS
    selective = n // 8
    blocks = _blocks(n)
    # `aiEmbed` batches, so with the default batch size a whole block is one call and the
    # count cannot show dedup. The dedup scenarios therefore pin batch size 1, which makes
    # one call per embedded input and the count meaningful.
    one_per_input = {"ai_function_embedding_max_batch_size": 1}
    return [
        # id, sql, ideal calls, extra settings
        ("L1", f"SELECT {CHAT} FROM lazy_rows WHERE id % 8 = 0 FORMAT Null", selective, {}),
        ("L2", f"SELECT {CHAT} FROM lazy_rows LIMIT 5 FORMAT Null", 5, {}),
        ("L3", f"SELECT {CHAT} FROM lazy_rows ORDER BY id LIMIT 5 FORMAT Null", 5, {}),
        (
            "L4",
            f"SELECT count() FROM lazy_rows WHERE id % 8 = 0 AND {CHAT} = 'positive' FORMAT Null",
            selective,
            {},
        ),
        (
            "L5",
            f"SELECT if(id % 8 = 0, {CHAT}, '') FROM lazy_rows FORMAT Null",
            selective,
            {"short_circuit_function_evaluation": "force_enable"},
        ),
        (
            "L6",
            f"SELECT {EMB} FROM lazy_dup FORMAT Null",
            DISTINCT_VALUES * blocks,
            one_per_input,
        ),
        (
            "L7",
            f"SELECT {EMB} FROM (SELECT DISTINCT x FROM lazy_dup) FORMAT Null",
            DISTINCT_VALUES,
            one_per_input,
        ),
        (
            "L8",
            f"WITH {EMB} AS a SELECT a, arraySum(a) FROM lazy_rows FORMAT Null",
            n,
            one_per_input,
        ),
        (
            # CSE is legal here: `aiEmbed` declares isDeterministic() = true. There is
            # deliberately no chat equivalent - `aiClassify` inherits isDeterministic() =
            # false, so reusing the filter's result for the projection is not something
            # the engine may do, and an "ideal" of N would never be reachable.
            "L9e",
            f"SELECT {EMB} FROM lazy_rows WHERE length({EMB}) > 0 FORMAT Null",
            n,
            one_per_input,
        ),
        ("L10", f"SELECT {CHAT} FROM lazy_rows PREWHERE id % 8 = 0 FORMAT Null", selective, {}),
    ]


@pytest.fixture(scope="module")
def laziness(q, mock, mock_tables):
    """Run every laziness scenario once and collect its call count."""
    mock.reset()
    measured = {}
    for case, sql, ideal, extra in _lazy_scenarios():
        settings = {"max_block_size": LAZY_BLOCK, "max_threads": 1}
        settings.update(extra)
        _result, events = q.run(sql, case=f"lazy_{case}", settings=settings)
        measured[case] = {"ideal": ideal, "current": events["api_calls"]}
    return measured


def test_laziness_baseline(laziness, cfg, request):
    """Assert no scenario got less lazy, and report how far each is from ideal."""
    name = "laziness"
    payload = {
        "_comment": (
            "AIAPICalls per query shape. Generated by AI_E2E_WRITE_BASELINES=1; "
            "bless a change only together with the code change that causes it."
        ),
        "rows": LAZY_ROWS,
        "max_block_size": LAZY_BLOCK,
        "distinct_values": DISTINCT_VALUES,
        "scenarios": laziness,
    }

    baseline = baselines.load(name)
    if cfg.write_baselines or baseline is None:
        path = baselines.save(name, payload)
        pytest.skip(f"wrote baseline {path}; rerun to assert against it")

    warning = baselines.staleness_warning(baseline)
    if warning:
        print(f"\n[ai-e2e] WARNING: {warning}")

    report = Report("laziness", {"rows": LAZY_ROWS, "block": LAZY_BLOCK})
    regressions = []
    for case, values in sorted(laziness.items()):
        known = baseline["scenarios"].get(case)
        ideal, current = values["ideal"], values["current"]
        report.add(
            case,
            ideal=ideal,
            current=current,
            baseline=known["current"] if known else "-",
            laziness=round(ideal / current, 3) if current else "-",
        )
        if known and current > known["current"]:
            regressions.append(
                f"{case}: {current} calls, baseline {known['current']}"
            )
    report.flush()
    print("\n" + report.render(["case", "ideal", "current", "baseline", "laziness"]))
    assert not regressions, "evaluation became less lazy: " + "; ".join(regressions)
