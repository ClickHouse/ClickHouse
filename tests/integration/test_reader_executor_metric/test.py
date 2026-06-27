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
import os
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

# Explicit [lo, hi] band per (metric x case) - a gross-regression gate, NOT a tight assertion
# (the deterministic unit grid is the precise value gate). cost/MiB is the headline
# load-independent KPI; R/I/O are diagnostic levers. Stateless I is gated by the I==0 invariant,
# not here. Warm cells are ~0, gated by invariants. The real-load metrics are NOISY: prewhere on
# a cold cache swings wildly within AND between runs (R 40-104, I 14-60, cv up to 0.44) and the
# fragmented cells vary with the random eviction pattern, so a single recompute under-samples the
# volatile cells. Regenerated (test_recompute_baseline) after the progressive fill-ahead run-ahead
# and the net-waste over-read metric: R dropped (the read-ahead coalesces fetches into fewer GETs)
# and O is now the NET over-read - the within-thread fetch-ahead is read back from the cache so it
# no longer counts, leaving only the cross-thread / stripe-boundary prefill. A single recompute
# under-samples the volatile cells (prewhere/cold), so CI on consistent hardware is the
# authoritative gate; re-tune a row from the CI metric report if it regresses. Update a row only
# when an executor change intentionally moves its magnitude.
BASELINE = {
    ("sequential", "cold", "live"): {"R": (15, 35), "I": (6, 15), "O": (13, 51), "cost/MiB": (11.6, 34.8)},
    ("sequential", "fragmented", "live"): {"R": (8, 24), "I": (5, 15), "O": (7, 38), "cost/MiB": (3.1, 12.3)},
    ("selective", "cold", "live"): {"R": (17, 39), "I": (1, 12), "O": (15, 60), "cost/MiB": (28.9, 86.7)},
    ("selective", "fragmented", "live"): {"R": (5, 14), "I": (0, 8), "O": (0, 0), "cost/MiB": (19.2, 76.6)},
    ("aggregation", "cold", "live"): {"R": (18, 43), "I": (6, 15), "O": (26, 103), "cost/MiB": (13.1, 39.2)},
    ("aggregation", "fragmented", "live"): {"R": (8, 24), "I": (4, 13), "O": (9, 50), "cost/MiB": (3.0, 12.2)},
    # prewhere/cold is the documented worst-case for variance (R and I swing widely run-to-run);
    # widened to the union of two recompute runs plus margin (R seen 32-103). The cell-aligned
    # single-tier fold abandons the per-cold-cell connection on a seek, raising I on the
    # seek-heavy selective/prewhere cells.
    ("prewhere", "cold", "live"): {"R": (20, 120), "I": (0, 20), "O": (10, 41), "cost/MiB": (17.2, 51.7)},
    ("prewhere", "fragmented", "live"): {"R": (30, 110), "I": (0, 12), "O": (0, 12), "cost/MiB": (8.0, 32.0)},
    ("sequential", "cold", "stateless"): {"R": (37, 87), "O": (10, 42), "cost/MiB": (13.7, 41.2)},
    ("sequential", "fragmented", "stateless"): {"R": (11, 34), "O": (7, 40), "cost/MiB": (3.4, 13.4)},
    ("selective", "cold", "stateless"): {"R": (21, 50), "O": (15, 60), "cost/MiB": (29.5, 88.6)},
    ("selective", "fragmented", "stateless"): {"R": (5, 15), "O": (0, 0), "cost/MiB": (18.3, 73.1)},
    ("aggregation", "cold", "stateless"): {"R": (43, 101), "O": (17, 69), "cost/MiB": (14.2, 42.5)},
    ("aggregation", "fragmented", "stateless"): {"R": (12, 35), "O": (5, 29), "cost/MiB": (3.1, 12.3)},
    ("prewhere", "cold", "stateless"): {"R": (35, 82), "O": (7, 28), "cost/MiB": (17.0, 50.9)},
    ("prewhere", "fragmented", "stateless"): {"R": (12, 60), "O": (0, 2), "cost/MiB": (4.1, 24.0)},
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


def _check_bands(name, state, mode, st, cost_per_mib):
    """Return any gated-metric violations (mean outside its recorded [lo, hi] band).
    Collected and asserted once at the end so a single run reports every cell rather
    than aborting at the first drift."""
    key = (name, state, mode)
    if key not in BASELINE:
        return []
    measured = {
        "R": st["R"][0],
        "I": st["I"][0],
        "O": st["O"][0] / (1024.0 * 1024.0),
        "cost/MiB": cost_per_mib,
    }
    violations = []
    for metric, (lo, hi) in BASELINE[key].items():
        got = measured[metric]
        if not (lo <= got <= hi):
            violations.append(f"{name}/{state}/{mode}: {metric}={got:.1f} outside [{lo}, {hi}]")
    return violations


def test_metric_values_and_stability(started_cluster):
    report = []
    band_violations = []
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

                # Loose magnitude gate against the recorded baseline (collected,
                # asserted once at the end).
                band_violations.extend(_check_bands(name, state, mode, st, cost_per_mib))

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

    assert not band_violations, "metric band violations:\n" + "\n".join(band_violations)


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


# Per-cache-state band margins used when regenerating the baseline. These are a loose
# gross-regression gate, not precise assertions: the scattered loads (selective, prewhere)
# swing wide run-to-run - the long connection opens and abandons variably on seek-heavy
# reads - so the margins are generous (and wider still for the fragmented half-warm state).
RECOMPUTE_MARGIN = {"cold": 0.40, "fragmented": 0.50}


def _band(values, margin, ndigits):
    """Suggest a [lo, hi] band that covers every sample and is at least +/-margin
    around the mean, rounded to ndigits (0 -> int)."""
    mean = sum(values) / len(values)
    lo = round(min(mean * (1.0 - margin), min(values)), ndigits)
    hi = round(max(mean * (1.0 + margin), max(values)), ndigits)
    return (int(lo), int(hi)) if ndigits == 0 else (lo, hi)


def test_recompute_baseline(started_cluster):
    """Disabled helper to regenerate the BASELINE dict after an intended metric change.

    Skipped unless RECOMPUTE_METRIC_BASELINE is set, so it never runs in CI. To run it,
    inject the variable through praktika's --param (a bare shell env var is not forwarded
    into the test container):

        python3 -m ci.praktika run integration --test test_reader_executor_metric \
            --param RECOMPUTE_METRIC_BASELINE=1

    It measures the same load x state x mode matrix the gate test uses and reports a
    ready-to-paste BASELINE dict. It deliberately fails to surface that dict (pytest
    hides output for passing tests), so a "failure" here is the expected outcome.
    Review the suggested bands before committing - small or near-zero counters (e.g.
    live `I`) may need a wider band than the margin alone gives."""
    if not os.environ.get("RECOMPUTE_METRIC_BASELINE"):
        pytest.skip("manual: set RECOMPUTE_METRIC_BASELINE=1 to regenerate the BASELINE dict")

    lines = ["BASELINE = {"]
    for mode, use_long_conn in MODES:
        for name, query in LOADS.items():
            for state in ("cold", "fragmented"):
                samples = _collect_samples(query, use_long_conn, state)
                margin = RECOMPUTE_MARGIN[state]
                cells = [("R", [s["R"] for s in samples], 0, margin)]
                if use_long_conn:  # stateless I is gated by the pool-side invariant, not a band
                    cells.append(("I", [s["I"] for s in samples], 0, margin))
                # Over-read bytes swing widely run-to-run (read-ahead alignment / task split),
                # more than a request count, so give O extra margin like cost/MiB below.
                cells.append(("O", [s["O"] / (1024.0 * 1024.0) for s in samples], 0, margin + 0.20))
                # cost/MiB is a composite ratio that compounds the R/I/O variance, so it
                # swings wider than any single counter - give it extra margin to keep the
                # band consistent with the (independently gated) component bands.
                cells.append(("cost/MiB", [_cost_per_mib(s) for s in samples], 1, margin + 0.10))
                body = ", ".join(f'"{metric}": {_band(vals, m, nd)}' for metric, vals, nd, m in cells)
                lines.append(f'    ("{name}", "{state}", "{mode}"): {{{body}}},')
    lines.append("}")
    text = "\n".join(lines)
    # Surface the dict in the report. The suite runs with
    # --report-log-exclude-logs-on-passed-tests, which hides captured stdout/logs for
    # passing tests, so fail on purpose to print the regenerated BASELINE - this
    # "failure" is the expected outcome when regenerating.
    pytest.fail("Regenerated BASELINE (copy into the module-level BASELINE dict):\n" + text)


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
