"""Real-load metric for the ReaderExecutor (`use_reader_executor`).

Drives a MergeTree on an S3 disk with a FileCache (production geometry: 32 MiB
segments, 4 MiB alignment) and reads the executor's own ProfileEvents per query
from `system.query_log`, then computes the same cost the unit grid uses:

    Cost_ms = 30*R + 5*I + 20*O_MiB + 0.1*Wc + 0.05*Rc

For each load x cache-state x connection-budget mode, repeated SAMPLES times, the
test reports each counter's value and stability (coefficient of variation), asserts
the mode-aware invariant (live: I == pool resets; stateless: I == 0), and checks each
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
}
ALL_EVENTS = {**METRIC_EVENTS, **POOL_EVENTS}

# Multi-threaded (matches the production load test) plus the connection-budget axis:
# reader_executor_use_live_connections = 1 (live: a reusable connection across windows)
# or 0 (stateless: a short-lived one-shot connection per window). max_threads=8 because
# the read pool hands each thread non-contiguous task ranges, which is where live
# connections get abandoned (the incomplete-connection / reset regime).
def _settings(live):
    return {
        "use_reader_executor": 1,
        "max_threads": 8,
        "reader_executor_use_live_connections": 1 if live else 0,
    }

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

# Explicit [lo, hi] band per (metric x case), derived from repeated runs as
# center +- max(2.5 x observed-half-range, 12%). Tight where the metric is stable
# (cost/MiB varies <=5% run-to-run, R/I cold are near-deterministic), wide only where
# the data demands it (over-read O on fragmented cells genuinely swings ~30-40%).
# cost/MiB is the headline load-independent KPI (modeled ms per MiB requested); R/I/O
# are the diagnostic levers. Stateless I is gated by the I==0 invariant, not here.
# The deterministic unit grid remains the tight value gate; update a row when an
# executor change intentionally moves its magnitudes. Warm cells are ~0, gated by invariants.
BASELINE = {
    ("sequential", "cold", "live"): {"R": (95, 122), "I": (56, 73), "O": (116, 149), "cost/MiB": (33, 42.1)},
    ("sequential", "cold", "stateless"): {"R": (171, 218), "O": (105, 135), "cost/MiB": (42.3, 53.9)},
    ("sequential", "fragmented", "live"): {"R": (45, 59), "I": (27, 40), "O": (47, 68), "cost/MiB": (15.3, 19.5)},
    ("sequential", "fragmented", "stateless"): {"R": (80, 102), "O": (33, 57), "cost/MiB": (18.8, 24.1)},
    ("selective", "cold", "live"): {"R": (29, 39), "I": (24, 32), "O": (58, 75), "cost/MiB": (112.2, 142.9)},
    ("selective", "cold", "stateless"): {"R": (53, 69), "O": (58, 75), "cost/MiB": (133.4, 169.9)},
    ("selective", "fragmented", "live"): {"R": (10, 14), "I": (10, 14), "O": (0, 1), "cost/MiB": (27.7, 35.4)},
    ("selective", "fragmented", "stateless"): {"R": (20, 27), "O": (0, 1), "cost/MiB": (36.8, 46.9)},
    ("aggregation", "cold", "live"): {"R": (157, 201), "I": (9, 18), "O": (54, 79), "cost/MiB": (39.6, 50.5)},
    ("aggregation", "cold", "stateless"): {"R": (265, 338), "O": (43, 77), "cost/MiB": (51.8, 66.1)},
    ("aggregation", "fragmented", "live"): {"R": (55, 71), "I": (18, 25), "O": (22, 55), "cost/MiB": (15.6, 20)},
    ("aggregation", "fragmented", "stateless"): {"R": (102, 130), "O": (11, 34), "cost/MiB": (20.2, 25.8)},
    ("prewhere", "cold", "live"): {"R": (30, 43), "I": (11, 16), "O": (36, 51), "cost/MiB": (25.7, 32.8)},
    ("prewhere", "cold", "stateless"): {"R": (206, 264), "O": (33, 47), "cost/MiB": (48.4, 61.7)},
    ("prewhere", "fragmented", "live"): {"R": (21, 31), "I": (7, 11), "O": (7, 10), "cost/MiB": (11.7, 15)},
    ("prewhere", "fragmented", "stateless"): {"R": (106, 136), "O": (7, 10), "cost/MiB": (23, 29.4)},
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


def _measure(query, live):
    qid = str(uuid.uuid4())
    node.query(query, query_id=qid, settings=_settings(live))
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


def _warm_even_blocks(live):
    # Warm the even 1/16 blocks of the table; the odd blocks stay cold. Touch every
    # column the loads read (v and k, plus id via the filter) so each load sees a
    # genuinely half-warm cache -> reads then alternate cache-hit / cold-miss (the
    # fragmented / checkerboard state).
    block = TABLE_ROWS // 16
    ranges = " OR ".join(
        f"(id >= {b * block} AND id < {(b + 1) * block})" for b in range(0, 16, 2)
    )
    node.query(f"SELECT sum(v), sum(k) FROM t WHERE {ranges}", settings=_settings(live))


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
    """Assert each gated metric's mean lands within its recorded [lo, hi] band."""
    key = (name, state, mode)
    if key not in BASELINE:
        return
    measured = {
        "R": st["R"][0],
        "I": st["I"][0],
        "O": st["O"][0] / (1024.0 * 1024.0),
        "cost/MiB": cost_per_mib,
    }
    for metric, (lo, hi) in BASELINE[key].items():
        got = measured[metric]
        assert lo <= got <= hi, (
            f"{name}/{state}/{mode}: {metric}={got:.1f} outside [{lo}, {hi}]")


def test_metric_values_and_stability(started_cluster):
    report = []
    for live in (True, False):
        mode = "live" if live else "stateless"
        for name, query in LOADS.items():
            for state in ("cold", "warm", "fragmented"):
                if state == "warm":
                    _drop_caches()
                    _measure(query, live)  # prime once, then measure the warm cache
                    samples = [_measure(query, live) for _ in range(SAMPLES)]
                elif state == "fragmented":
                    # Re-establish the half-warm cache per sample (measuring it would
                    # otherwise populate the cold blocks and turn the cache fully warm).
                    samples = []
                    for _ in range(SAMPLES):
                        _drop_caches()
                        _warm_even_blocks(live)
                        samples.append(_measure(query, live))
                else:  # cold
                    samples = []
                    for _ in range(SAMPLES):
                        _drop_caches()
                        samples.append(_measure(query, live))

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

                # Mode-aware invariant. Live: every executor-counted incomplete connection
                # is a real S3 pool reset (I == ConnReset). Stateless: no reused connection
                # to abandon -> I == 0 (one short-lived bounded connection per window).
                for s in samples:
                    if live:
                        assert s["I"] == s["ConnReset"], (
                            f"{name}/{state}/live: executor I={s['I']} != pool ConnReset={s['ConnReset']}")
                    else:
                        assert s["I"] == 0, (
                            f"{name}/{state}/stateless: expected no incomplete connections, got I={s['I']}")
                    # The server's modeled-cost event must equal the formula recomputed from the raw
                    # counters (only integer-us truncation on the bandwidth term differs).
                    assert abs(_cost_ms(s) - _formula_cost_ms(s)) <= max(2.0, 0.005 * _formula_cost_ms(s)), (
                        f"{name}/{state}/{mode}: cost event {_cost_ms(s):.1f}ms != formula {_formula_cost_ms(s):.1f}ms")
                # Sanity: the executor actually ran (no legacy fallback).
                if state == "cold" and name == "sequential" and live:
                    assert st["R"][0] > 0, "executor did not run (R=0 cold sequential -> likely legacy fallback)"

                # Loose magnitude gate against the recorded baseline.
                _check_bands(name, state, mode, st, cost_per_mib)

    banner = "\n=== ReaderExecutor real-load metric (state x pattern x mode) ===\n" + "\n".join(report) + "\n"
    logging.info(banner)
    print(banner)
