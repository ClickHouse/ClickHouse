-- Tags: no-ordinary-database, no-replicated-database
-- no-ordinary-database, no-replicated-database: a refreshable materialized view with a
-- non-replicated inner table requires an Atomic database.

DROP TABLE IF EXISTS rmv_04888 SYNC;
DROP TABLE IF EXISTS mv_04888 SYNC;
DROP TABLE IF EXISTS src_04888 SYNC;

SELECT '-- a refreshable view must be dropped: a skipped DROP leaves it refreshing forever';

CREATE MATERIALIZED VIEW rmv_04888 REFRESH EVERY 1 YEAR (x UInt64)
ENGINE = MergeTree ORDER BY x EMPTY AS SELECT 1 AS x;

DROP TABLE rmv_04888 SETTINGS ignore_drop_queries_probability = 1;
SELECT countIf(name = 'rmv_04888'), countIf(name LIKE '.inner_id.%')
FROM system.tables WHERE database = currentDatabase();

SELECT '-- a plain materialized view is a user DROP and is still skipped';

CREATE TABLE src_04888 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_04888 (x UInt64) ENGINE = MergeTree ORDER BY x
AS SELECT x FROM src_04888;

DROP TABLE mv_04888 SETTINGS ignore_drop_queries_probability = 1;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04888';

DROP TABLE mv_04888 SYNC;
DROP TABLE src_04888 SYNC;
