-- Tags: shard
-- `ABR` chooses its table from the filters of the query, which a serialized query plan does not carry. With
-- `serialize_query_plan = 1`, a remote server that reads an `ABR` table is sent the SQL of the query instead of a plan.

SET allow_experimental_abr = 1;
SET enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0;

DROP VIEW IF EXISTS abr_view_of_view;
DROP VIEW IF EXISTS abr_view;
DROP TABLE IF EXISTS dist_abr;
DROP TABLE IF EXISTS dist_serialized_10;
DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS serialized_1;
DROP TABLE IF EXISTS serialized_10;

CREATE TABLE serialized_1 (ts DateTime, key UInt64, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE serialized_10 (ts DateTime, key UInt64, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;

-- Both tables cover the same range, so the queries go to interval 1. `value` tells which table was read.
INSERT INTO serialized_1 SELECT toDateTime('2026-06-01 00:00:00') + number, number, 1 FROM numbers(100);
INSERT INTO serialized_10 SELECT toDateTime('2026-06-01 00:00:00') + number, number, 10 FROM numbers(100);

CREATE TABLE abr ENGINE = ABR('serialized', [1, 10], ('ts'));
CREATE TABLE dist_abr AS abr ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'abr');
CREATE TABLE dist_serialized_10 AS serialized_10 ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'serialized_10');

SELECT count(), max(value) FROM remote('127.0.0.2', currentDatabase(), 'abr')
WHERE ts >= toDateTime('2026-06-01 00:00:00')
SETTINGS log_comment = '05357_remote';

SELECT key % 2 AS k, sum(value) FROM dist_abr
WHERE ts >= toDateTime('2026-06-01 00:00:00')
GROUP BY k ORDER BY k
SETTINGS log_comment = '05357_distributed';

-- The `ABR` table is only read by the subquery of `IN`, which runs on the shard.
SELECT count() FROM dist_serialized_10
WHERE key IN (SELECT key FROM abr WHERE ts >= toDateTime('2026-06-01 00:01:00'));

-- Without inlining, a view is read as the view itself, which passes the filter of the outer query to the `ABR` table it
-- reads, directly or through another view.
CREATE VIEW abr_view AS SELECT * FROM abr;
CREATE VIEW abr_view_of_view AS SELECT ts, value FROM abr_view;

SELECT count(), max(value) FROM remote('127.0.0.2', currentDatabase(), 'abr_view')
WHERE ts >= toDateTime('2026-06-01 00:00:00')
SETTINGS analyzer_inline_views = 0;

SELECT count(), max(value) FROM remote('127.0.0.2', currentDatabase(), 'abr_view_of_view')
WHERE ts >= toDateTime('2026-06-01 00:00:00')
SETTINGS analyzer_inline_views = 0;

-- The same through the `view` table function, which is planned as a read of a table function rather than of a table.
SELECT count(), max(value) FROM remote('127.0.0.2', view(SELECT * FROM {CLICKHOUSE_DATABASE:Identifier}.abr))
WHERE ts >= toDateTime('2026-06-01 00:00:00')
SETTINGS analyzer_inline_views = 0;

SELECT count(), max(value) FROM remote('127.0.0.2', view(SELECT ts, value FROM {CLICKHOUSE_DATABASE:Identifier}.abr_view))
WHERE ts >= toDateTime('2026-06-01 00:00:00')
SETTINGS analyzer_inline_views = 0;

-- The queries on the shard chose interval 1 from their time filter.
-- The shard runs in the default database of its connection, so its entries are found through the initial queries.
SYSTEM FLUSH LOGS query_log;
SELECT abr_interval
FROM system.query_log
WHERE type = 'QueryFinish'
    AND query_kind = 'Select'
    AND NOT is_initial_query
    AND event_date >= yesterday()
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND type = 'QueryFinish'
            AND is_initial_query
            AND log_comment IN ('05357_remote', '05357_distributed')
            AND event_date >= yesterday());

DROP VIEW abr_view_of_view;
DROP VIEW abr_view;
DROP TABLE dist_abr;
DROP TABLE dist_serialized_10;
DROP TABLE abr;
DROP TABLE serialized_1;
DROP TABLE serialized_10;
