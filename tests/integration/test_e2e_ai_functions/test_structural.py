"""Suite B, structural half: exact integers against the mock at `delay_ms = 0`.

These cases assert `AIAPICalls` and request counts, never wall-clock, so they are cheap,
deterministic, tolerant of a shared host, and safe to run in parallel with anything. The
timing cases live in `test_latency_arch.py`.

Call-count invariants that need no endpoint live in the mock suite instead
(`tests/integration/test_ai_functions/test.py`: `test_api_call_count_per_query_shape` and
`test_api_call_quota_is_per_query`). They are exact integers on any host, so CI validates
them on every pull request rather than whenever someone remembers to run this suite. What
stays here needs instrumentation that suite's mock does not have: connection counting and
injected delay.
"""

import math

import pytest

from . import corpus as ai_corpus
from .conftest import load_table

pytestmark = pytest.mark.e2e

CATEGORIES = "['positive','negative','neutral']"
CHAT = f"aiClassify(x, {CATEGORIES}, map('credentials','ai_e2e_mock_chat'))"
EMB = "aiEmbed(x, 'mock-model', map('credentials','ai_e2e_mock_embed'))"


@pytest.fixture(scope="module")
def mock_tables(instance):
    """Tables used by the mock-driven cases."""
    load_table(
        instance, "struct_rows", [("id", "UInt32"), ("x", "String")], ai_corpus.mock_rows(32)
    )
    yield
    for table in ("struct_rows",):
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
