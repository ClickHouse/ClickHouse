-- Tags: no-parallel, shard
-- - no-parallel: uses a fail point (global server state)
-- - shard: needs the test cluster to build a `Distributed` table

DROP TABLE IF EXISTS t04827_local SYNC;
DROP TABLE IF EXISTS t04827_dist SYNC;
DROP VIEW IF EXISTS v04827_join;

CREATE TABLE t04827_local (k UInt64, rv String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t04827_local SELECT number, concat('r', toString(number)) FROM numbers(8);

CREATE TABLE t04827_dist (k UInt64, rv String)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t04827_local);

-- The join and its settings live in a view, so the assertions below and the query that reproduces
-- the bug are the same SQL by construction: neither `WITH TOTALS` nor `join_algorithm` can be
-- changed for one and not the other.
CREATE VIEW v04827_join AS
SELECT count() AS rows_joined, countIf(rv != '') AS right_rows_seen
FROM (SELECT number AS k FROM numbers(8)) AS l
FULL JOIN (SELECT k, rv FROM t04827_dist GROUP BY k, rv WITH TOTALS) AS r USING (k)
SETTINGS join_algorithm = 'partial_merge', skip_unavailable_shards = 1,
         prefer_localhost_replica = 0, async_socket_for_remote = 0,
         async_query_sending_for_remote = 0, max_threads = 1;

-- The reproduction is defined by things being absent, so assert both ingredients first. The join
-- has to run on `MergeJoin`, and it has to have a totals port: `FillingRightJoinSide` reports two
-- inputs (right side plus totals) only when the right pipeline has totals, and reports no input
-- count otherwise.
SELECT '-- the join under test runs on MergeJoin and has a totals input';
SELECT extract(explain, 'Algorithm: (\\w+)') FROM (EXPLAIN PLAN actions = 1 SELECT * FROM v04827_join)
WHERE explain ILIKE '%Algorithm:%';
SELECT count() FROM (EXPLAIN PIPELINE SELECT * FROM v04827_join)
WHERE explain ILIKE '%FillingRightJoinSide 2 %';

SYSTEM ENABLE FAILPOINT remote_query_executor_cancel_before_send;

-- The cancelled shard never sends a Totals packet, so the totals port asserted above delivers zero
-- chunks. The right side must still be finalized: reading it unmerged dereferences a null
-- `RowBitmaps`.
-- The result is the oracle, so it is printed rather than discarded: `8 0` means the shard was
-- cancelled as intended, and `8 8` would mean the right rows arrived and the starved-totals path
-- was never taken. A throwing timeout keeps an incomplete execution from passing as success.
SELECT '-- starved totals port';
SELECT * FROM v04827_join
SETTINGS max_execution_time = 60, timeout_overflow_mode = 'throw';

-- The fail point is once-only, so it disables itself when consumed. A still-enabled fail point
-- here would mean the cancellation never fired and the query above proved nothing.
SELECT count() FROM system.fail_points
WHERE enabled AND name = 'remote_query_executor_cancel_before_send';

SYSTEM DISABLE FAILPOINT remote_query_executor_cancel_before_send;

-- The server must still be alive (the null dereference is fatal under UBSan).
SELECT 'ok';

DROP VIEW v04827_join;
DROP TABLE t04827_local SYNC;
DROP TABLE t04827_dist SYNC;
