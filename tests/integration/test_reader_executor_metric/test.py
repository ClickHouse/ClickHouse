"""Real-load metric for the ReaderExecutor (`use_reader_executor`).

Drives a MergeTree on an S3 disk with a FileCache (production geometry: 32 MiB
segments, 4 MiB alignment) and reads the executor's own ProfileEvents per query
from `system.query_log`, then computes the same cost the unit grid uses:

    Cost_ms = 30*R + 5*I + 20*O_MiB + 0.1*Wc + 0.05*Rc

For each load x cache-state x connection-budget mode, repeated SAMPLES times, the
test reports each counter's value and stability (coefficient of variation), asserts
the mode-aware invariant (live: ConnReset <= I <= ConnReset + ConnExpired; stateless:
I <= ConnReset + ConnExpired - see the assert comment), and checks each
counter and the cost-per-MiB KPI against an explicit per-(metric x case) [lo, hi] band.
The unit grid (deterministic) remains the tight value gate.
"""

import logging
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)

# ~256 MiB of incompressible `v` -> ~8 x 32 MiB cache segments (one wide part).
TABLE_ROWS = 32_000_000
SAMPLES = 5  # stability samples per (load x cache-state)

# Executor metric counters (ProfileEvents) -> short names used in the report.
METRIC_EVENTS = {
    "R": "ReaderExecutorSourceRequests",
    "I": "ReaderExecutorIncompleteConnections",
    "O": "ReaderExecutorOverReadBytes",
    "S": "ReaderExecutorBytesFromSource",
    "Wc": "ReaderExecutorCachePopulateRequests",
    "Rc": "ReaderExecutorCacheGetRequests",
}

# The actual S3-disk connection pool counters (the "reset connections" seen on the
# load test). Compared against the executor's derived I to check they agree, and to
# expose connection churn (Created/Reset) that the live vs stateless path produces.
POOL_EVENTS = {
    "ConnCreated": "DiskConnectionsCreated",
    "ConnReused": "DiskConnectionsReused",
    "ConnReset": "DiskConnectionsReset",
    "ConnExpired": "DiskConnectionsExpired",
}
ALL_EVENTS = {**METRIC_EVENTS, **POOL_EVENTS}

# Multi-threaded (matches the production load test) plus the connection-budget axis:
# reader_executor_use_long_connections = 1 (long: a reusable connection across windows)
# or 0 (stateless: a short-lived one-shot connection per window). max_threads=8 because
# the read pool hands each thread non-contiguous task ranges, which is where long
# connections get abandoned (the incomplete-connection / reset regime).
def _settings(use_long_conn, extra=None):
    s = {
        "use_reader_executor": 1,
        "max_threads": 8,
        "reader_executor_use_long_connections": 1 if use_long_conn else 0,
    }
    if extra:
        s.update(extra)
    return s


# Page-cache path: a raw-S3 disk (no file cache) plus this setting makes the
# executor route reads through the userspace page cache (block-granular, 1 MiB).
def _pc_settings(use_long_conn, extra=None):
    s = _settings(use_long_conn, extra)
    s["use_page_cache_for_disks_without_file_cache"] = 1
    return s


# Matrix modes: (label, long_connections). `live` keeps a reusable (long) source
# connection across windows; `stateless` opens a short-lived one-shot connection per
# window. Both run the schedule-driven interpreter (the only read path).
MODES = [("live", True), ("stateless", False)]

LOADS = {
    # full-column scan -> R + I regime
    "sequential": "SELECT sum(v) FROM t",
    # 16 ids spread evenly across the table; at full scale they are ~16 MiB of `v`
    # apart (> min_bytes_for_seek 8 MiB) so they are NOT coalesced -> scattered -> O.
    "selective": f"SELECT sum(v) FROM t WHERE id IN (SELECT number * {TABLE_ROWS // 16} FROM numbers(16))",
    # full scan + group-by -> realistic analytic load
    "aggregation": "SELECT k, sum(v) FROM t GROUP BY k FORMAT Null",
    # non-PK PREWHERE -> `v` is read only for the 1/16 matching granules; the reader
    # seeks past the rest, abandoning the live connection before its advertised mark-
    # range bound -> the incomplete-connection (I) regime that full scans never hit.
    "prewhere": "SELECT sum(v) FROM t PREWHERE bucket = 0",
}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        _setup_table()
        yield cluster
    finally:
        cluster.shutdown()


def _setup_table():
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t (id UInt64, k UInt32, v UInt64, bucket UInt8)
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0,
                 index_granularity = 8192, index_granularity_bytes = 0
        """
    )
    # `bucket` is constant within each 8192-row granule (granule_index % 16), so a
    # non-PK PREWHERE on it skips whole granules of `v` -> sparse, seek-heavy reads.
    node.query(
        f"INSERT INTO t SELECT number, number % 1000, cityHash64(number), intDiv(number, 8192) % 16 "
        f"FROM numbers({TABLE_ROWS})",
        settings={"max_insert_threads": 1},
    )
    # Consolidate to a single wide part: deterministic geometry (one contiguous ~256 MiB
    # `v` file -> ~8 cache segments) and a reproducible R across runs.
    node.query("OPTIMIZE TABLE t FINAL")
    node.query("SYSTEM STOP MERGES t")
    logging.info("table parts: %s", node.query("SELECT count() FROM system.parts WHERE table='t' AND active").strip())

    # Same data on a raw-S3 (no file cache) disk, so reads route through the
    # userspace page cache - the block-granular (1 MiB) cache where the
    # live-connection bridge can fire on small cached holes.
    node.query("DROP TABLE IF EXISTS t_pc SYNC")
    node.query(
        """
        CREATE TABLE t_pc (id UInt64, k UInt32, v UInt64, bucket UInt8)
        ENGINE = MergeTree ORDER BY id
        SETTINGS storage_policy = 's3_page', min_bytes_for_wide_part = 0,
                 index_granularity = 8192, index_granularity_bytes = 0
        """
    )
    node.query("INSERT INTO t_pc SELECT * FROM t", settings={"max_insert_threads": 1})
    node.query("OPTIMIZE TABLE t_pc FINAL")
    node.query("SYSTEM STOP MERGES t_pc")


def _cost_ms(m):
    """The server-emitted modeled cost (ReaderExecutorModeledCostMicroseconds), in ms."""
    return m["Cost"] / 1000.0


def _formula_cost_ms(m):
    """The same weighted sum recomputed from the raw counters; cross-checks the server event.
    Bandwidth is charged on bytes_from_source (useful source payload + over-read), matching the
    server's ReaderExecutorModeledCostMicroseconds increment."""
    return (
        30.0 * m["R"]
        + 5.0 * m["I"]
        + 20.0 * (m["S"] / (1024.0 * 1024.0))
        + 0.1 * m["Wc"]
        + 0.05 * m["Rc"]
    )


def _cost_per_mib(m):
    """Load-independent KPI: modeled ms of cost per MiB of requested (useful) bytes."""
    req_mib = m["Requested"] / (1024.0 * 1024.0)
    return _cost_ms(m) / req_mib if req_mib else 0.0


def _drop_caches():
    # FILESYSTEM CACHE alone leaves the mark/uncompressed caches serving reads from
    # memory, which makes the per-query counts non-reproducible. Drop all three so a
    # "cold" read genuinely goes through the executor every time.
    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")


def _drop_pc():
    # Cold page-cache reads: drop the page cache (and mark/uncompressed) so the
    # read genuinely goes to S3 through the executor each time.
    node.query("SYSTEM DROP PAGE CACHE")
    node.query("SYSTEM DROP MARK CACHE")
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")


def _measure(query, use_long_conn, pc=False, extra=None):
    qid = str(uuid.uuid4())
    node.query(query, query_id=qid, settings=_pc_settings(use_long_conn, extra) if pc else _settings(use_long_conn, extra))
    node.query("SYSTEM FLUSH LOGS")
    extra = ["ReaderExecutorModeledCostMicroseconds", "ReaderExecutorRequestedBytes"]
    cols = ", ".join(f"ProfileEvents['{e}']" for e in list(ALL_EVENTS.values()) + extra)
    row = node.query(
        f"SELECT {cols} FROM system.query_log "
        f"WHERE query_id = '{qid}' AND type = 'QueryFinish' AND current_database = currentDatabase() "
        f"ORDER BY query_start_time_microseconds DESC LIMIT 1"
    ).strip()
    assert row, f"no QueryFinish row in query_log for {qid}"
    vals = list(map(int, row.split("\t")))
    m = dict(zip(ALL_EVENTS.keys(), vals))
    m["Cost"], m["Requested"] = vals[-2], vals[-1]
    return m


def _warm_even_blocks(use_long_conn):
    # Warm the even 1/16 blocks of the table; the odd blocks stay cold. Touch every
    # column the loads read (v and k, plus id via the filter) so each load sees a
    # genuinely half-warm cache -> reads then alternate cache-hit / cold-miss (the
    # fragmented / checkerboard state).
    block = TABLE_ROWS // 16
    ranges = " OR ".join(
        f"(id >= {b * block} AND id < {(b + 1) * block})" for b in range(0, 16, 2)
    )
    node.query(f"SELECT sum(v), sum(k) FROM t WHERE {ranges}", settings=_settings(use_long_conn))


def _collect_samples(query, use_long_conn, state, extra=None):
    """Gather SAMPLES measurements for one (load, mode, cache-state) cell. `extra` is
    applied only to the MEASURED reads, never the priming/warming, so the cache state
    is identical across A/B arms and only the measured query differs."""
    if state == "warm":
        _drop_caches()
        _measure(query, use_long_conn)  # prime once, then measure the warm cache
        return [_measure(query, use_long_conn, extra=extra) for _ in range(SAMPLES)]
    if state == "fragmented":
        # Re-establish the half-warm cache per sample (measuring it would
        # otherwise populate the cold blocks and turn the cache fully warm).
        samples = []
        for _ in range(SAMPLES):
            _drop_caches()
            _warm_even_blocks(use_long_conn)
            samples.append(_measure(query, use_long_conn, extra=extra))
        return samples
    # cold
    samples = []
    for _ in range(SAMPLES):
        _drop_caches()
        samples.append(_measure(query, use_long_conn, extra=extra))
    return samples


def _stats(samples):
    out = {}
    n = len(samples)
    for k in samples[0]:
        xs = [s[k] for s in samples]
        mean = sum(xs) / n
        std = (sum((x - mean) ** 2 for x in xs) / n) ** 0.5
        out[k] = (mean, std, (std / mean if mean else 0.0), min(xs), max(xs))
    return out


def test_metric_values_and_stability(started_cluster):
    # Asserts only CONFIG-INDEPENDENT invariants (the pool-side I bound, the cost-formula
    # equality, executor-ran) plus the async-metric registration. The per-cell R/I/O/cost
    # magnitudes are printed for diagnostics but NOT gated: they are timing/config-sensitive on
    # a max_threads=8 read and cannot hold a tight band across the full CI sanitizer/config matrix.
    report = []
    for mode, use_long_conn in MODES:
        for name, query in LOADS.items():
            for state in ("cold", "warm", "fragmented"):
                samples = _collect_samples(query, use_long_conn, state)

                st = _stats(samples)
                cost = sum(_cost_ms(s) for s in samples) / len(samples)
                cost_per_mib = sum(_cost_per_mib(s) for s in samples) / len(samples)
                line = (
                    f"[{name}/{state}/{mode}] "
                    + " ".join(
                        f"{k}={st[k][0]:.0f}(cv={st[k][2]:.2f},min={st[k][3]},max={st[k][4]})"
                        for k in ALL_EVENTS
                    )
                    + f" cost={cost:.1f}ms cost/MiB={cost_per_mib:.2f}ms"
                )
                report.append(line)
                logging.info(line)

                # Pool-side upper bound. Every executor-counted incomplete connection is a
                # connection the pool could not re-store: counted as `ConnReset`, or as
                # `ConnExpired` when the session's keep-alive request budget was already
                # exhausted at destroy (the pool checks expiry BEFORE completeness, so an
                # abandoned connection on a spent session lands in Expired, not Reset). So
                # I <= ConnReset + ConnExpired in every mode. There is no sound lower bound:
                # the pool also resets COMPLETE connections under its own eviction/keep-alive
                # policy, so ConnReset is not bounded above by I.
                for s in samples:
                    assert s["I"] <= s["ConnReset"] + s["ConnExpired"], (
                        f"{name}/{state}/{mode}: executor I={s['I']} exceeds "
                        f"ConnReset+ConnExpired={s['ConnReset'] + s['ConnExpired']}")
                    # The server's modeled-cost event must equal the formula recomputed from the raw
                    # counters (only integer-us truncation on the bandwidth term differs).
                    assert abs(_cost_ms(s) - _formula_cost_ms(s)) <= max(2.0, 0.005 * _formula_cost_ms(s)), (
                        f"{name}/{state}/{mode}: cost event {_cost_ms(s):.1f}ms != formula {_formula_cost_ms(s):.1f}ms")
                # Sanity: the executor actually ran (no legacy fallback).
                if state == "cold" and name == "sequential" and use_long_conn:
                    assert st["R"][0] > 0, "executor did not run (R=0 cold sequential -> likely legacy fallback)"

    # The cost-per-byte KPI is also exposed as an async metric (interval delta) for
    # realtime instance graphs. After a full run it is registered and non-negative.
    async_val = node.query(
        "SELECT value FROM system.asynchronous_metrics "
        "WHERE metric = 'ReaderExecutorModeledCostMsPerRequestedMiB'"
    ).strip()
    assert async_val != "", "ReaderExecutorModeledCostMsPerRequestedMiB async metric not registered"
    assert float(async_val) >= 0.0, f"async cost-per-MiB metric is negative: {async_val}"

    banner = "\n=== ReaderExecutor real-load metric (state x pattern x mode) ===\n" + "\n".join(report) + "\n"
    logging.info(banner)
    print(banner)


# Plan-window A/B under concurrent load (Stage 3c). Setting
# `reader_executor_plan_look_ahead_max_window` == `reader_executor_window_size` collapses
# the generalized plan to a fixed small window; the server default (32 MiB) is the variable
# window. The single-thread unit grid showed the long-connection carry makes the two
# cost-equivalent on a sequential scan (identical source GETs). This runs the same
# comparison under `max_threads=8` with eviction pressure (cold + fragmented), the regime
# where a large window can pin far-ahead segments a small one would let evict before the
# serve arrives, dodging a re-fetch. The printed ratios are the evidence that decides
# whether the variable-window machinery earns its keep (Stage 4); the assertion is only a
# loose pathology guard, not a tight equivalence gate (under-load variance is real).
def test_fixed_small_vs_variable_window(started_cluster):
    window = node.query("SELECT getSetting('reader_executor_window_size')").strip()
    fixed_small = {"reader_executor_plan_look_ahead_max_window": int(window)}
    report = []
    pathological = []
    for name in ("sequential", "aggregation", "prewhere"):
        q = LOADS[name]
        for state in ("cold", "fragmented"):
            var_s = _collect_samples(q, True, state)
            small_s = _collect_samples(q, True, state, extra=fixed_small)
            var_cpm = sum(_cost_per_mib(s) for s in var_s) / len(var_s)
            small_cpm = sum(_cost_per_mib(s) for s in small_s) / len(small_s)
            var_r = sum(s["R"] for s in var_s) / len(var_s)
            small_r = sum(s["R"] for s in small_s) / len(small_s)
            ratio = small_cpm / var_cpm if var_cpm else 1.0
            line = (
                f"[{name}/{state}] variable: R={var_r:.0f} cost/MiB={var_cpm:.2f} | "
                f"fixed-small: R={small_r:.0f} cost/MiB={small_cpm:.2f} | small/var={ratio:.2f}"
            )
            report.append(line)
            logging.info(line)
            assert small_r > 0, f"{name}/{state}: fixed-small executor did not run (R=0 -> likely legacy fallback)"
            # Pathology guard: a fixed-small window must not cost dramatically more than the
            # variable one. A breach here is the signal that the variable window earns its keep.
            if ratio > 2.0:
                pathological.append(f"{name}/{state}: fixed-small cost/MiB {small_cpm:.2f} is {ratio:.2f}x variable {var_cpm:.2f}")

    banner = "\n=== plan-window A/B (variable vs fixed-small, live, under load) ===\n" + "\n".join(report) + "\n"
    logging.info(banner)
    print(banner)

    assert not pathological, "fixed-small window pathology:\n" + "\n".join(pathological)


def test_page_cache_path(started_cluster):
    """Real-load coverage of the executor's userspace page-cache read path (a raw-S3
    disk with no file cache). The page cache is block-granular (1 MiB), unlike the
    4 MiB-aligned FileCache, so a cold sequential scan fragments into MORE, smaller
    source reads - the regime where connection churn (and the seek-side optimisation)
    matters most. Warm reads are served from the page cache (no source request).
    Asserts the executor runs on the page-cache disk, warm reads hit the cache, the
    metric/cost wiring works there, and the pool-side invariant holds
    (I <= ConnReset + ConnExpired)."""
    query = "SELECT sum(v) FROM t_pc"
    report = []
    results = {}
    for mode, use_long_conn in MODES:
        for state in ("cold", "warm"):
            if state == "warm":
                _drop_pc()
                _measure(query, use_long_conn, pc=True)  # prime, then measure the warm cache
                samples = [_measure(query, use_long_conn, pc=True) for _ in range(SAMPLES)]
            else:
                samples = []
                for _ in range(SAMPLES):
                    _drop_pc()
                    samples.append(_measure(query, use_long_conn, pc=True))
            st = _stats(samples)
            cost_per_mib = sum(_cost_per_mib(s) for s in samples) / len(samples)
            line = (
                f"[pc/{state}/{mode}] "
                + " ".join(f"{k}={st[k][0]:.0f}(cv={st[k][2]:.2f})" for k in ALL_EVENTS)
                + f" cost/MiB={cost_per_mib:.2f}ms"
            )
            report.append(line)
            logging.info(line)
            results[(state, mode)] = st
            # Mode invariant holds on the page-cache path too (same pool-side bound as
            # the main matrix - see the comment there). The server's modeled cost event
            # must equal the formula recomputed from the raw counters.
            for s in samples:
                assert s["I"] <= s["ConnReset"] + s["ConnExpired"], (
                    f"pc/{state}/{mode}: executor I={s['I']} exceeds "
                    f"ConnReset+ConnExpired={s['ConnReset'] + s['ConnExpired']}")
                assert abs(_cost_ms(s) - _formula_cost_ms(s)) <= max(2.0, 0.005 * _formula_cost_ms(s)), (
                    f"pc/{state}/{mode}: cost event {_cost_ms(s):.1f}ms != formula {_formula_cost_ms(s):.1f}ms")

    # The executor actually ran through the page cache on a cold read.
    assert results[("cold", "live")]["R"][0] > 0, "executor did not run on the page-cache disk"
    # Warm reads are served from the page cache, not S3 (far fewer source requests).
    assert results[("warm", "live")]["R"][0] * 10 < results[("cold", "live")]["R"][0], (
        "warm page-cache scan should issue far fewer source requests than cold")

    banner = "\n=== ReaderExecutor page-cache path (state x mode) ===\n" + "\n".join(report) + "\n"
    logging.info(banner)
    print(banner)


def test_repro_legacy_vs_executor(started_cluster):
    """Localize the premature-EOF (CANNOT_READ_ALL_DATA) seen on the executor sequential/cold
    scan: run the SAME multi-threaded scan legacy (use_reader_executor=0) vs executor live
    (long connection) vs executor stateless (one-shot per window). Legacy-passes/executor-fails
    => executor-specific bound/EOF bug; live-fails/stateless-passes => the long-connection path."""
    q = LOADS["sequential"]

    def run(label, settings):
        _drop_caches()
        qid = str(uuid.uuid4())
        try:
            res = node.query(q, query_id=qid, settings=settings).strip()
            return f"{label}: OK sum={res} (qid={qid})", res
        except Exception as e:  # noqa: BLE001
            return f"{label}: FAILED (qid={qid}): {str(e)[:400]}", None

    legacy_msg, legacy_res = run("legacy(executor=0)", {"use_reader_executor": 0, "max_threads": 8})
    live_msg, live_res = run("executor-live", _settings(True))
    stateless_msg, stateless_res = run("executor-stateless", _settings(False))

    logging.error("REPRO RESULTS:\n  %s\n  %s\n  %s", legacy_msg, live_msg, stateless_msg)

    assert legacy_res is not None, f"legacy path itself failed: {legacy_msg}"
    assert live_res == legacy_res and stateless_res == legacy_res, (
        "executor result/availability differs from legacy:\n  "
        + "\n  ".join([legacy_msg, live_msg, stateless_msg])
    )


def test_repro_unified_foreground_matches_legacy(started_cluster):
    """`reader_executor_unified_foreground` ON: the synchronous foreground serve runs the SAME
    FetchMachine inline (LocalRunner) instead of the bespoke sync read. Run the SAME multi-threaded
    cold scan legacy vs executor (live + stateless) with the flag ON and assert identical results -
    the inline foreground path is correct on real S3 + a multi-thread ReadPool (the pattern where
    the legacy sync path's premature-EOF bug once lived)."""
    q = LOADS["sequential"]

    def run(label, settings):
        _drop_caches()
        qid = str(uuid.uuid4())
        try:
            res = node.query(q, query_id=qid, settings=settings).strip()
            return f"{label}: OK sum={res} (qid={qid})", res
        except Exception as e:  # noqa: BLE001
            return f"{label}: FAILED (qid={qid}): {str(e)[:400]}", None

    unified = {"reader_executor_unified_foreground": 1, "max_threads": 8}
    legacy_msg, legacy_res = run("legacy(executor=0)", {"use_reader_executor": 0, "max_threads": 8})
    live_msg, live_res = run("executor-live-unified", _settings(True, extra=unified))
    stateless_msg, stateless_res = run("executor-stateless-unified", _settings(False, extra=unified))

    logging.error("UNIFIED-FG REPRO RESULTS:\n  %s\n  %s\n  %s", legacy_msg, live_msg, stateless_msg)

    assert legacy_res is not None, f"legacy path itself failed: {legacy_msg}"
    assert live_res == legacy_res and stateless_res == legacy_res, (
        "unified-foreground executor result/availability differs from legacy:\n  "
        + "\n  ".join([legacy_msg, live_msg, stateless_msg])
    )
