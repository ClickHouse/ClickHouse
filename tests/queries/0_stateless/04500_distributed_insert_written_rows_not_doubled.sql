-- Tags: no-parallel, no-replicated-database, distributed
-- Tag reason: creates a Distributed table over a two-shards-localhost cluster and checks
-- global system.query_log / system.view_refreshes accounting.

SET allow_experimental_refreshable_materialized_view = 1;

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
