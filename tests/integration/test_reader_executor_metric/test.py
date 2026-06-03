"""Real-load metric for the ReaderExecutor (`use_reader_executor`).

Drives a MergeTree on an S3 disk with a FileCache (production geometry: 32 MiB
segments, 4 MiB alignment) and reads the executor's own ProfileEvents per query
from `system.query_log`, then computes the same cost the unit grid uses:

    Cost_ms = 30*R + 5*I + 20*O_MiB + 0.1*Wc + 0.05*Rc

For several loads x cache-states, each repeated SAMPLES times, the test reports
the value and the stability (coefficient of variation) of each counter. Reads
are single-threaded so the counts are deterministic for a fixed cache state,
which isolates "is the metric reproducible" from thread-scheduling races.
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
    return (
        30.0 * m["R"]
        + 5.0 * m["I"]
        + 20.0 * (m["O"] / (1024.0 * 1024.0))
        + 0.1 * m["Wc"]
        + 0.05 * m["Rc"]
    )


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
    cols = ", ".join(f"ProfileEvents['{e}']" for e in ALL_EVENTS.values())
    row = node.query(
        f"SELECT {cols} FROM system.query_log "
        f"WHERE query_id = '{qid}' AND type = 'QueryFinish' AND current_database = currentDatabase() "
        f"ORDER BY query_start_time_microseconds DESC LIMIT 1"
    ).strip()
    assert row, f"no QueryFinish row in query_log for {qid}"
    return dict(zip(ALL_EVENTS.keys(), map(int, row.split("\t"))))


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
                line = (
                    f"[{name}/{state}/{mode}] "
                    + " ".join(
                        f"{k}={st[k][0]:.0f}(cv={st[k][2]:.2f},min={st[k][3]},max={st[k][4]})"
                        for k in ALL_EVENTS
                    )
                    + f" cost={cost:.1f}ms"
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
                # Sanity: the executor actually ran (no legacy fallback).
                if state == "cold" and name == "sequential" and live:
                    assert st["R"][0] > 0, "executor did not run (R=0 cold sequential -> likely legacy fallback)"

    banner = "\n=== ReaderExecutor real-load metric (state x pattern x mode) ===\n" + "\n".join(report) + "\n"
    logging.info(banner)
    print(banner)
