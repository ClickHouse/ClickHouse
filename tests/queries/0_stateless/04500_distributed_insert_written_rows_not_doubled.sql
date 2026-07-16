-- Tags: no-parallel, no-replicated-database, distributed, no-parallel-replicas
-- Tag reason: creates a Distributed table over a two-shards-localhost cluster and checks
-- global system.query_log / system.view_refreshes accounting. The exact written_rows /
-- written_bytes values depend on the distributed execution path, so pin the routing settings
-- that select the local-shard synchronous path this test exercises. no-parallel-replicas keeps
-- the runner from injecting enable_parallel_replicas + cluster_for_parallel_replicas, which
-- reroutes the INSERT / the system.query_log SELECT through a cluster with no local replica.

SET allow_experimental_refreshable_materialized_view = 1;

-- Deterministically select the local-shard, synchronous, no-parallel-replicas path. Without
-- these, randomized distributed settings change the accounting and the test flakes.
SET distributed_foreground_insert = 1;
SET prefer_localhost_replica = 1;
SET parallel_distributed_insert_select = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS local_04500;
DROP TABLE IF EXISTS dist_04500;

CREATE TABLE local_04500 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_04500 AS local_04500
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_04500, rand());

-- A plain INSERT into a Distributed table must not double-count the rows written to local
-- shards (issue #106361). Rows pushed into local shards are already accounted by the
-- top-level CountingTransform of the distributed INSERT.
INSERT INTO dist_04500 SELECT number FROM numbers(1000)
    SETTINGS log_comment = '04500_dist_insert';

SYSTEM FLUSH LOGS query_log;

-- Expect written_rows = 1000, not 2000, and written_bytes = 8000 (1000 UInt64), not 16000.
-- written_bytes comes from the same nested CountingTransform accounting that also drives the
-- WRITTEN_BYTES quota, so a correct value here confirms the quota is not double-charged either.
SELECT written_rows, written_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query
  AND query_kind = 'Insert'
  AND current_database = currentDatabase()
  AND log_comment = '04500_dist_insert'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Sanity: the 1000 inserted rows are split across shards (both pointing at the same
-- localhost table), so exactly 1000 physical rows exist -- confirming the 2000 the
-- unpatched server reported in written_rows was double-counting, not real rows.
SELECT count() FROM local_04500;

-- Refreshable MV appending into a Distributed target: system.view_refreshes.written_rows
-- must report the logical row count, not the doubled per-shard count.
DROP TABLE local_04500;
DROP TABLE dist_04500;
CREATE TABLE local_04500 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_04500 AS local_04500
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_04500, rand());

CREATE MATERIALIZED VIEW mv_04500 REFRESH EVERY 1 YEAR APPEND TO dist_04500 AS
    SELECT number AS x FROM numbers(1000);
SYSTEM WAIT VIEW mv_04500;

-- Expect read_rows = 1000, written_rows = 1000 (not 2000).
SELECT read_rows, written_rows
FROM system.view_refreshes
WHERE database = currentDatabase() AND view = 'mv_04500';

DROP TABLE mv_04500;
DROP TABLE dist_04500;
DROP TABLE local_04500;

-- Suppressing the nested per-shard accounting must NOT leak into a downstream materialized
-- view attached to the local shard table. A synchronous INSERT must still include the view's
-- own writes in system.query_views_log and charge its WRITTEN_BYTES quota (the flag is only
-- consulted by InterpreterInsertQuery, not by the view CountingTransform in
-- InsertDependenciesBuilder::createSelect).
DROP TABLE IF EXISTS mv_tgt_04500;
DROP VIEW IF EXISTS mv_dl_04500;
CREATE TABLE local_04500 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_04500 AS local_04500
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_04500, rand());
CREATE TABLE mv_tgt_04500 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_dl_04500 TO mv_tgt_04500 AS SELECT x FROM local_04500;

INSERT INTO dist_04500 SELECT number FROM numbers(1000)
    SETTINGS log_comment = '04500_dist_insert_mv', distributed_foreground_insert = 1;

SYSTEM FLUSH LOGS query_log, query_views_log;

-- The initial synchronous INSERT accounts the local-shard target write (1000) plus the
-- downstream view write (1000) = 2000, matching a plain (non-distributed) INSERT into a table
-- that has a materialized view. The point is that the per-shard write is not double-counted
-- (would be 3000) and the view write is not dropped (would be 1000).
SELECT written_rows, written_bytes
FROM system.query_log
WHERE type = 'QueryFinish'
  AND is_initial_query
  AND query_kind = 'Insert'
  AND current_database = currentDatabase()
  AND log_comment = '04500_dist_insert_mv'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- The materialized view's own write must be reported (1000, not dropped to 0).
SELECT sum(written_rows)
FROM system.query_views_log
WHERE view_name = currentDatabase() || '.mv_dl_04500';

-- Physical rows: 1000 in the target, 1000 in the view target.
SELECT count() FROM local_04500;
SELECT count() FROM mv_tgt_04500;

DROP VIEW mv_dl_04500;
DROP TABLE mv_tgt_04500;
DROP TABLE dist_04500;
DROP TABLE local_04500;

-- Suppressing the nested per-shard accounting must NOT leak into a delegating storage that
-- writes through its own nested InterpreterInsertQuery. TimeSeries is the concrete case:
-- StorageTimeSeries::write -> TimeSeriesSink::createTargetPipeline does Context::createCopy and
-- builds Tags/Samples/Metrics target inserts, which DO consult the flag. If it leaked, those
-- real target writes would lose their CountingTransforms and the distributed INSERT would
-- under-report written_rows/written_bytes versus a direct insert into the same TimeSeries table.
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_direct_04500;
DROP TABLE IF EXISTS ts_local_04500;
DROP TABLE IF EXISTS ts_dist_04500;

-- Baseline: direct insert into a TimeSeries table.
CREATE TABLE ts_direct_04500 ENGINE = TimeSeries;
INSERT INTO ts_direct_04500 (metric_name, tags, time_series)
    SELECT 'm' || toString(number % 50), map('h', toString(number)),
           [(toDateTime64(number, 3), toFloat64(number))]
    FROM numbers(1000)
    SETTINGS log_comment = '04500_ts_direct';

-- Distributed over a TimeSeries table with local shards.
CREATE TABLE ts_local_04500 ENGINE = TimeSeries;
CREATE TABLE ts_dist_04500 AS ts_local_04500
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), ts_local_04500, rand());
INSERT INTO ts_dist_04500 (metric_name, tags, time_series)
    SELECT 'm' || toString(number % 50), map('h', toString(number)),
           [(toDateTime64(number, 3), toFloat64(number))]
    FROM numbers(1000)
    SETTINGS log_comment = '04500_ts_dist', distributed_foreground_insert = 1;

SYSTEM FLUSH LOGS query_log;

-- The distributed insert must report the SAME written_rows as the direct insert: the TimeSeries
-- child inserts (samples/tags/metrics target tables) are still counted, not suppressed by the
-- one-shot flag. written_rows is the number of physical rows written to the three target tables
-- (deterministic); written_bytes is not compared because the distributed path re-blocks the data,
-- so its byte total legitimately differs from a direct insert. 1 means the row totals are equal.
SELECT
    (SELECT written_rows FROM system.query_log
       WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Insert'
         AND current_database = currentDatabase() AND log_comment = '04500_ts_dist'
       ORDER BY event_time_microseconds DESC LIMIT 1)
    =
    (SELECT written_rows FROM system.query_log
       WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Insert'
         AND current_database = currentDatabase() AND log_comment = '04500_ts_direct'
       ORDER BY event_time_microseconds DESC LIMIT 1);

-- The distributed insert must count strictly more than the outer 1000 rows and charge nonzero
-- bytes: without counting the TimeSeries child inserts it would report only 1000 (the
-- metric_name/tags/time_series rows) and a much smaller written_bytes.
SELECT written_rows > 1000 AND written_bytes > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND query_kind = 'Insert'
  AND current_database = currentDatabase() AND log_comment = '04500_ts_dist'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE ts_dist_04500;
DROP TABLE ts_local_04500;
DROP TABLE ts_direct_04500;
