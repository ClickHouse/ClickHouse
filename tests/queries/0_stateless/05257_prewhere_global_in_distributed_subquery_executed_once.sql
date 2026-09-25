-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/122122
-- A `GLOBAL IN` subquery is evaluated once and sent to the shards as a temporary table. Written in
-- an explicit `PREWHERE` on a `Distributed` table, the initiator used to evaluate it a second time
-- as well, for a set that nothing on the initiator ever reads, doubling the subquery's cost and the
-- rows it read.

-- Makes every shard query appear in `system.query_log`: with the default, the local shard is read
-- in-process and is not logged as a query of its own.
SET prefer_localhost_replica = 0;
-- The count below matches the shard queries by their text, which exists only when a shard receives
-- the query as text rather than as a serialized plan.
SET serialize_query_plan = 0;

CREATE TABLE t_05257 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05257 SELECT number FROM numbers(100);
CREATE TABLE d_05257 AS t_05257 ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05257);

-- Subject: the subquery must run once per shard, not twice.
SELECT count() FROM d_05257 PREWHERE k GLOBAL IN (SELECT k FROM d_05257 WHERE k < 10)
SETTINGS log_comment = '05257_prewhere';

-- Positive control, expected to be identical before and after: the same predicate in `WHERE` always
-- ran once per shard. It calibrates the count, so 2 reads as "one run per shard" rather than as "the
-- counting query stopped matching anything".
SELECT count() FROM d_05257 WHERE k GLOBAL IN (SELECT k FROM d_05257 WHERE k < 10)
SETTINGS log_comment = '05257_where';

-- The same shape reached through `remote(...)` instead of a `Distributed` table.
SELECT count() FROM remote('127.0.0.2,127.0.0.3', currentDatabase(), t_05257)
PREWHERE k GLOBAL IN (SELECT k FROM remote('127.0.0.2,127.0.0.3', currentDatabase(), t_05257) WHERE k < 10)
SETTINGS log_comment = '05257_remote_prewhere';

-- Control, expected to be identical before and after: a local table's reader evaluates `PREWHERE`
-- itself, so its set is still built. 10 of the 100 rows match.
SELECT count() FROM t_05257 PREWHERE k IN (SELECT k FROM t_05257 WHERE k < 10);

-- Control, expected to be identical before and after: the default configuration reads the local
-- shard in-process, so its shard queries are not comparable and only the result is asserted.
SELECT count() FROM d_05257 PREWHERE k GLOBAL IN (SELECT k FROM d_05257 WHERE k < 10)
SETTINGS prefer_localhost_replica = 1;

-- A `Merge` over a local table and a `Distributed` one reports itself as remote while still reading
-- the local table here, so that read needs the set. Withholding it aborts the query instead of
-- returning 30.
SELECT count() FROM merge(currentDatabase(), '^(t_05257|d_05257)$')
PREWHERE k GLOBAL IN (SELECT k FROM t_05257 WHERE k < 10);

-- A shard that receives the query as a serialized plan rather than as text evaluates `PREWHERE` in
-- that plan, so the set is needed there even though the table read is remote. Withholding it aborts
-- the query instead of returning 20.
SELECT count() FROM remote('127.0.0.2', currentDatabase(), d_05257)
PREWHERE k GLOBAL IN (SELECT k FROM t_05257 WHERE k < 10)
SETTINGS serialize_query_plan = 1;

SYSTEM FLUSH LOGS query_log;

-- Shard-side executions of the `GLOBAL IN` subquery, per outer query. Two shards, so one run per
-- shard is 2. The secondary queries do not carry `current_database`, so match them by the database
-- they read.
SELECT log_comment, countIf(query LIKE '%< 10%') AS subquery_runs_on_shards
FROM system.query_log
WHERE has(databases, currentDatabase())
    AND event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment IN ('05257_prewhere', '05257_where', '05257_remote_prewhere')
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE t_05257;
DROP TABLE d_05257;
