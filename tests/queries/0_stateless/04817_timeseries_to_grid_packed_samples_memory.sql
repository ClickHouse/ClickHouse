-- Tags: no-fasttest, no-parallel, no-asan, no-tsan, no-msan, no-ubsan, no-debug, no-random-settings, no-random-merge-tree-settings
-- ^^ this test pins a memory limit: sanitizers and randomized settings distort memory accounting, other tests running in parallel distort the server-level tracker, and the fast-test runner is too small for the 8M-row fixture.

-- The memory regression guard for the packed per-bucket sample storage of `timeSeries*ToGrid`: 8 series x 1M samples aggregated over a 2001-point grid keep every in-window sample in the aggregation state, which costs >130 MiB with raw 16-byte samples (the query below fails on the raw-buffering implementation) and under 35 MiB with sealed buckets packed into delta-encoded blobs.

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_packed_memory;
CREATE TABLE ts_packed_memory (id UInt64, ts DateTime64(3), v Float64) ENGINE = MergeTree ORDER BY (id, ts);

-- A counter scraped every second with jittered integer increments around 150 and a reset every 100k samples.
INSERT INTO ts_packed_memory
SELECT
    number % 8 AS id,
    fromUnixTimestamp64Milli(1767225600000 + intDiv(number, 8) * 1000) AS ts,
    toFloat64(150 * (intDiv(number, 8) % 100000) + intHash64(number) % 50) AS v
FROM numbers_mt(8000000)
SETTINGS max_insert_threads = 4;

SELECT id, arrayCount(x -> isNotNull(x), r) AS defined_points, round(arraySum(arrayMap(x -> ifNull(x, 0.), r)), 3) AS rate_sum
FROM
(
    SELECT id, timeSeriesRateToGrid(toDateTime64('2026-01-01 00:00:00', 3), toDateTime64('2026-01-12 13:46:40', 3), 500, 1000)(ts, v) AS r
    FROM ts_packed_memory GROUP BY id
)
ORDER BY id
SETTINGS max_threads = 4, max_memory_usage = '100Mi';

DROP TABLE ts_packed_memory;
