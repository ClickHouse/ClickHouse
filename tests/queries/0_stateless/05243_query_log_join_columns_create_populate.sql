-- Tags: no-old-analyzer, no-replicated-database
-- Tag no-replicated-database: `CREATE ... POPULATE` is not supported

-- A case of the join columns of `system.query_log`, split out of 05042_query_log_join_columns_more_cases.sql
-- so that the rest of those cases keep running on `Replicated` databases.
-- `POPULATE` runs the `SELECT` of the view as part of the `CREATE`, and the join it executes is reported
-- in the row of the `CREATE` query.

SET log_queries = 1;

DROP VIEW IF EXISTS mv_populate;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS t2;

CREATE TABLE src (a UInt64) ENGINE = Memory;
CREATE TABLE t2 (a UInt64, b UInt64) ENGINE = Memory;
INSERT INTO src SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number, number FROM numbers(10);

CREATE MATERIALIZED VIEW mv_populate ENGINE = Memory POPULATE AS SELECT src.a AS a FROM src JOIN t2 ON src.a = t2.a
SETTINGS log_comment = '05243_join_views_mv_populate', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;

SELECT query_kind, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND type = 'QueryFinish'
  AND log_comment = '05243_join_views_mv_populate';

DROP VIEW mv_populate;
DROP TABLE t2;
DROP TABLE src;
