"""Suite B, timing half: measures the *shape* of `T ~ rows x D` with `D` injected.

Not a latency test of an endpoint. The mock's delay is known, so what is measured is how
total query time relates to it: serial execution, parallelism, batching. Every case here
is timing-sensitive and wants an unshared host; the exact-integer cases live in
`test_structural.py`.

B1 is a report, B2 is a gate. Where a gate compares against `baselines/arch.json`, the
baseline holds integers and dimensionless ratios only - never wall-clock milliseconds,
which are host-dependent.
"""

import math
import threading
import time

import pytest

from . import baselines
from . import corpus as ai_corpus
from .asserts import Report
from .conftest import load_table

pytestmark = pytest.mark.e2e

CATEGORIES = "['positive','negative','neutral']"
CHAT = f"aiClassify(x, {CATEGORIES}, map('credentials','ai_e2e_mock_chat'))"
GEN = "aiGenerate(x, map('credentials','ai_e2e_mock_chat'))"
EMB = "aiEmbed(x, 'mock-model', map('credentials','ai_e2e_mock_embed'))"

# B1 is a matrix, so its per-cell cost multiplies. 32 rows at the default 200 ms delay is
# ~6.4 s of injected time per serial cell, which keeps the whole matrix in single-digit
# minutes while still being long enough for parallelism to show.
B1_ROWS = 32
GATE_ROWS = 32


def _load(instance, name, rows, parts=1):
    """Load `rows` rows, optionally spread over `parts` parts.

    `load_table` optimizes to a single part, so multi-part tables are built by inserting
    in chunks afterwards and skipping the merge.
    """
    data = ai_corpus.mock_rows(rows)
    if parts <= 1:
        load_table(instance, name, [("id", "UInt32"), ("x", "String")], data)
        return
    load_table(instance, name, [("id", "UInt32"), ("x", "String")], [])
    per_part = math.ceil(rows / parts)
    for start in range(0, rows, per_part):
        chunk = data[start : start + per_part]
        values = ", ".join(f"({row['id']}, '{row['x']}')" for row in chunk)
        instance.query(f"INSERT INTO {name} VALUES {values}")


@pytest.fixture(scope="module")
def arch_tables(instance):
    _load(instance, "arch_one_part", B1_ROWS, parts=1)
    _load(instance, "arch_many_parts", B1_ROWS, parts=8)
    _load(instance, "arch_gate", GATE_ROWS, parts=1)
    _load(instance, "arch_gate_parts", GATE_ROWS, parts=8)
    yield
    for table in ("arch_one_part", "arch_many_parts", "arch_gate", "arch_gate_parts"):
        instance.query(f"DROP TABLE IF EXISTS {table} SYNC")


@pytest.fixture(scope="module")
def timings(cfg, instance):
    """The host-dependent half of Suite B: reported, diffed run-local, never committed.

    `AI_E2E_COMPARE_TO=<a previous run's JSON>` turns this into a before/after table. That
    is the whole regression mechanism for wall-clock and CPU numbers: comparing them
    against a committed value would only ever measure the machine.
    """
    report = Report(
        "latency_arch",
        {
            "rows": B1_ROWS,
            "delay_ms": cfg.mock_delay_ms,
            "git_sha": baselines.current_sha()[:12],
            "nproc": instance.exec_in_container(["nproc"]).strip(),
        },
    )
    yield report
    path = report.flush()
    if cfg.compare_to:
        table, regressions = report.compare(
            cfg.compare_to, columns=("duration_ms", "cpu_us_per_row")
        )
        print("\n" + table)
        if regressions:
            worst = max(regressions, key=lambda item: item[4])
            print(
                f"\n[ai-e2e] slowest regression: {worst[0]} {worst[1]} "
                f"{worst[2]} -> {worst[3]} ({(worst[4] - 1) * 100:+.1f}%)"
            )
    else:
        print(
            f"\n[ai-e2e] wrote {path}. Re-run with AI_E2E_COMPARE_TO=<that path> after a "
            "change to get a before/after table on this host."
        )


def _effective_concurrency(calls, delay_ms, duration_ms):
    """C = (calls x D) / T. 1.0 means fully serial; P means P requests overlapped."""
    if not duration_ms:
        return 0.0
    return round((calls * delay_ms) / duration_ms, 2)


# ---------------------------------------------------------------------------
# B1 - characterization. Report only, no assertions.
# ---------------------------------------------------------------------------


def test_b1_characterization(q, mock, cfg, arch_tables, timings):
    delay = cfg.mock_delay_ms
    report = timings

    cells = []
    for function, expr, extra in (
        ("aiGenerate", GEN, {}),
        ("aiEmbed", EMB, {"ai_function_embedding_max_batch_size": 1}),
    ):
        for threads in (1, 8):
            for table, parts in (("arch_one_part", 1), ("arch_many_parts", 8)):
                for block in (8, B1_ROWS):
                    cells.append((function, expr, extra, threads, table, parts, block))

    for function, expr, extra, threads, table, parts, block in cells:
        mock.reset()
        mock.configure(delay_ms=delay)
        settings = {"max_threads": threads, "max_block_size": block}
        settings.update(extra)
        _result, events = q.run(
            f"SELECT {expr} FROM {table} FORMAT Null",
            case=f"b1_{function}_{threads}t_{parts}p_{block}b",
            settings=settings,
        )
        stats = mock.stats()
        report.add(
            f"{function} threads={threads} parts={parts} block={block}",
            api_calls=events["api_calls"],
            duration_ms=events["query_duration_ms"],
            max_in_flight=stats["max_in_flight"],
            mean_in_flight=stats["mean_in_flight"],
            connections=stats["connections"],
            effective_concurrency=_effective_concurrency(
                events["api_calls"], delay, events["query_duration_ms"]
            ),
            serial_ratio=round(
                events["query_duration_ms"] / max(1, events["api_calls"] * delay), 2
            ),
        )

    print(
        "\n"
        + report.render(
            [
                "case",
                "api_calls",
                "duration_ms",
                "max_in_flight",
                "mean_in_flight",
                "connections",
                "effective_concurrency",
                "serial_ratio",
            ]
        )
    )


# ---------------------------------------------------------------------------
# B2-1 - serial baseline. One-sided: with one request per row issued serially,
# T >= rows x D always, so a lower bound could only fire on parallelism, which is
# what B2-4 exists to bless.
# ---------------------------------------------------------------------------


def test_b2_1_serial_upper_bound(q, mock, cfg, arch_tables):
    delay = cfg.mock_delay_ms
    mock.reset()
    mock.configure(delay_ms=delay)
    _result, events = q.run(
        f"SELECT {GEN} FROM arch_gate FORMAT Null",
        case="b2_1",
        settings={"max_threads": 1, "max_block_size": GATE_ROWS},
    )
    assert events["api_calls"] == GATE_ROWS
    limit = GATE_ROWS * delay * 1.4
    assert events["query_duration_ms"] <= limit, (
        f"{events['query_duration_ms']} ms for {GATE_ROWS} rows at {delay} ms "
        f"(limit {limit:.0f} ms): extra requests, per-row overhead, or a sleep"
    )


# ---------------------------------------------------------------------------
# B2-3 (timing) - a batched embedding query should take about one delay per batch.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("batch", [1, 8, 32])
def test_b2_3_batch_timing(q, mock, cfg, arch_tables, batch):
    delay = cfg.mock_delay_ms
    mock.reset()
    mock.configure(delay_ms=delay)
    _result, events = q.run(
        f"SELECT {EMB} FROM arch_gate FORMAT Null",
        case=f"b2_3_timing_{batch}",
        settings={
            "max_threads": 1,
            "max_block_size": GATE_ROWS,
            "ai_function_embedding_max_batch_size": batch,
        },
    )
    calls = math.ceil(GATE_ROWS / batch)
    assert events["api_calls"] == calls
    expected = calls * delay
    assert events["query_duration_ms"] <= expected * 1.4 + 2000, (
        f"batch={batch}: {events['query_duration_ms']} ms vs ~{expected} ms of "
        "injected delay"
    )


# ---------------------------------------------------------------------------
# B2-4 - parallelism. The blessed baseline is whatever the implementation does
# today; the day a change adds parallelism, the new value is blessed and can
# never silently regress.
# ---------------------------------------------------------------------------


def test_b2_4_parallelism_not_lost(q, mock, cfg, arch_tables):
    delay = cfg.mock_delay_ms
    mock.reset()
    mock.configure(delay_ms=delay)
    _result, events = q.run(
        f"SELECT {GEN} FROM arch_gate_parts FORMAT Null",
        case="b2_4",
        settings={"max_threads": 8, "max_block_size": 4},
    )
    stats = mock.stats()
    measured = {
        "max_in_flight_8t": stats["max_in_flight"],
        "effective_concurrency_8t": _effective_concurrency(
            events["api_calls"], delay, events["query_duration_ms"]
        ),
    }

    baseline = baselines.load("arch")
    if cfg.write_baselines or baseline is None or "max_in_flight_8t" not in baseline:
        merged = dict(baseline or {})
        merged.update(measured)
        merged.setdefault(
            "_comment",
            "Integers and dimensionless ratios only; wall-clock is compared run-local.",
        )
        path = baselines.save("arch", merged)
        pytest.skip(f"wrote baseline {path}; rerun to assert against it")

    warning = baselines.staleness_warning(baseline)
    if warning:
        print(f"\n[ai-e2e] WARNING: {warning}")

    print(
        f"\n[ai-e2e] parallelism: max_in_flight={measured['max_in_flight_8t']} "
        f"(baseline {baseline['max_in_flight_8t']}), effective concurrency="
        f"{measured['effective_concurrency_8t']} "
        f"(baseline {baseline['effective_concurrency_8t']})"
    )
    assert measured["max_in_flight_8t"] >= baseline["max_in_flight_8t"], (
        "fewer requests overlap than the baseline: parallelism was lost"
    )
    assert (
        measured["effective_concurrency_8t"]
        >= baseline["effective_concurrency_8t"] * 0.8
    ), "effective concurrency dropped more than 20% below the baseline"


# ---------------------------------------------------------------------------
# B2-6 - cancellation. Expected to fail today: the row loop has no cancellation
# checkpoint, so a block cannot be interrupted until it completes. Non-strict so
# an unexpected pass (someone added a checkpoint) is not itself an error.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=False,
    reason="no cancellation checkpoint in the AI row loop; see 02-latency.md section 9",
)
def test_b2_6_kill_query_latency(q, mock, cfg, instance, arch_tables):
    mock.reset()
    mock.configure(delay_ms=15000)
    query_id = "b2_6_kill"
    failures = []

    def run_query():
        try:
            instance.query(
                f"SELECT {GEN} FROM arch_gate LIMIT 4 FORMAT Null",
                settings=q.settings(
                    {
                        "max_threads": 1,
                        "max_block_size": 4,
                        "ai_function_request_timeout_sec": 120,
                    }
                ),
                query_id=query_id,
                timeout=600,
            )
        except Exception as error:  # killed queries raise, which is the expected path
            failures.append(str(error))

    worker = threading.Thread(target=run_query, daemon=True)
    worker.start()
    time.sleep(2)

    instance.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")
    started = time.monotonic()
    deadline = started + cfg.kill_budget_sec
    while time.monotonic() < deadline:
        running = instance.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if running == "0":
            break
        time.sleep(0.2)
    elapsed = time.monotonic() - started
    worker.join(timeout=300)

    print(f"\n[ai-e2e] KILL QUERY took {elapsed:.1f}s (budget {cfg.kill_budget_sec}s)")
    assert elapsed <= cfg.kill_budget_sec, (
        f"query survived {elapsed:.1f}s after KILL; the row loop has no cancellation "
        "checkpoint, so the block runs to completion"
    )


# ---------------------------------------------------------------------------
# B2-7 / B2-8 - a throttling endpoint. Correctness is the gate, not in-flight:
# with serial execution `max_in_flight` is 1 and any bound on it is vacuous.
# ---------------------------------------------------------------------------


def test_b2_7_throttling_endpoint_still_correct(q, mock, cfg, arch_tables):
    mock.reset()
    # Injected rejections rather than a concurrency limit: the chat path is serial within a
    # block, so a limit of 2 may never be reached and this case would pass without testing
    # anything. Reject fewer times than `max_retries`, so every row still succeeds.
    rejections = 3
    mock.configure(
        delay_ms=50, reject_next_n=rejections, reject_status=429, echo_token=True
    )
    # `WHERE NOT ignore(...)` forces per-row evaluation. Wrapping the AI call in a
    # subquery under `count()` does not: the optimizer prunes the unused projection and no
    # request is ever sent - the same methodology trap the competitive benchmark hit.
    _result, events = q.run(
        f"SELECT count() FROM arch_gate_parts WHERE NOT ignore({GEN}) FORMAT TSV",
        case="b2_7",
        settings={
            "max_threads": 8,
            "max_block_size": 2,
            "ai_function_max_retries": 3,
            "ai_function_retry_initial_delay_ms": 50,
        },
    )
    stats = mock.stats()
    print(
        f"\n[ai-e2e] throttled endpoint: {stats['over_limit_rejections']} rejections "
        f"absorbed by retries"
    )
    assert stats["over_limit_rejections"] == rejections, (
        "the injected rejections never happened - if 0, the AI call was optimized away"
    )
    assert events["rows_processed"] == GATE_ROWS, "every row must still get a result"
    assert events["rows_skipped"] == 0
    # Every attempt counts, so the retried requests show up on top of one call per row.
    assert events["api_calls"] == GATE_ROWS + rejections, (
        f"AIAPICalls={events['api_calls']}, expected {GATE_ROWS} rows + {rejections} "
        "retried attempts"
    )


def test_b2_8_throttling_without_retries_throws(q, mock, arch_tables):
    mock.reset()
    mock.configure(delay_ms=0, reject_next_n=1, reject_status=429)
    error = q.error(
        f"SELECT {GEN} FROM arch_gate FORMAT Null",
        case="b2_8",
        settings={
            "max_threads": 1,
            "max_block_size": GATE_ROWS,
            "ai_function_max_retries": 0,
        },
    )
    stats = mock.stats()
    assert stats["over_limit_rejections"] == 1, "the injected rejection never happened"
    assert error, "a rejected request with no retries configured must fail the query"
    assert "429" in error or "rate limit" in error.lower(), (
        f"the error should carry the status: {error[:200]}"
    )


# ---------------------------------------------------------------------------
# B2-9 - per-row CPU cost at D = 0. Wall-clock is unusable here: the mock's own
# per-request cost is the same order as ClickHouse's per-row cost, so this reads
# the query's CPU counter instead.
# ---------------------------------------------------------------------------


def test_b2_9_per_row_cpu_cost(q, mock, cfg, instance, arch_tables, timings):
    rows = 512
    _load(instance, "arch_cpu", rows, parts=1)
    mock.reset()
    mock.configure(delay_ms=0, output_tokens=4)

    samples = []
    for attempt in range(5):
        _result, events = q.run(
            f"SELECT {GEN} FROM arch_cpu FORMAT Null",
            case=f"b2_9_{attempt}",
            settings={"max_threads": 1, "max_block_size": rows},
        )
        assert events["api_calls"] == rows
        samples.append(events["user_time_us"] / rows)
    samples.sort()
    median = round(samples[len(samples) // 2], 1)
    instance.query("DROP TABLE IF EXISTS arch_cpu SYNC")

    timings.add("cpu per row", cpu_us_per_row=median, rows=rows)
    print(f"\n[ai-e2e] CPU per row: {median} us (host {instance.exec_in_container(['nproc']).strip()} cores)")

    # Deliberately no committed baseline and no absolute threshold: this is CPU time on
    # one host with one build, so a number from another machine would measure the machine.
    # The regression check is the run-local before/after diff printed by the `timings`
    # fixture. Only a sanity floor is asserted here.
    assert median > 0, "no CPU time attributed to the query"
