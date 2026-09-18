-- Tags: no-ordinary-database, no-replicated-database
-- no-ordinary-database: a non-APPEND refreshable materialized view is refused on an
-- Ordinary database regardless of the inner table.
-- no-replicated-database: a Replicated database refuses a non-APPEND refreshable
-- materialized view whose inner table is not replicated.

DROP TABLE IF EXISTS rmv_04913 SYNC;
DROP TABLE IF EXISTS mv_04913 SYNC;
DROP TABLE IF EXISTS src_04913 SYNC;

SELECT '-- a refreshable view must be dropped: a skipped DROP leaves it refreshing forever';

CREATE MATERIALIZED VIEW rmv_04913 REFRESH EVERY 1 YEAR (x UInt64)
ENGINE = MergeTree ORDER BY x EMPTY AS SELECT 1 AS x;

DROP TABLE rmv_04913 SETTINGS ignore_drop_queries_probability = 1;
SELECT countIf(name = 'rmv_04913'), countIf(name LIKE '.inner_id.%')
FROM system.tables WHERE database = currentDatabase();

SELECT '-- a plain materialized view is a user DROP and is still skipped';

CREATE TABLE src_04913 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_04913 (x UInt64) ENGINE = MergeTree ORDER BY x
AS SELECT x FROM src_04913;

DROP TABLE mv_04913 SETTINGS ignore_drop_queries_probability = 1;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04913';

DROP TABLE mv_04913 SYNC;
DROP TABLE src_04913 SYNC;
