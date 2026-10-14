-- `engine_full` of a materialized view with an inner table is the engine of that inner table.
-- https://github.com/ClickHouse/ClickHouse/issues/90700

DROP TABLE IF EXISTS mv_inner;
DROP TABLE IF EXISTS mv_inner_with_clauses;
DROP TABLE IF EXISTS mv_to;
DROP TABLE IF EXISTS plain_view;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
CREATE TABLE dst (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW mv_inner (x UInt8)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192
    AS SELECT x FROM src;

CREATE MATERIALIZED VIEW mv_inner_with_clauses (x UInt8)
    ENGINE = SummingMergeTree PARTITION BY x % 2 ORDER BY x SETTINGS index_granularity = 1024
    AS SELECT x FROM src;

-- A materialized view with `TO` has no inner table, and an ordinary view has no engine at all.
CREATE MATERIALIZED VIEW mv_to TO dst AS SELECT x FROM src;
CREATE VIEW plain_view AS SELECT x FROM src;

SELECT name, engine, engine_full
FROM system.tables
WHERE database = currentDatabase() AND name NOT LIKE '.inner%' AND engine IN ('MaterializedView', 'View')
ORDER BY name;

DROP TABLE mv_inner;
DROP TABLE mv_inner_with_clauses;
DROP TABLE mv_to;
DROP TABLE plain_view;
DROP TABLE dst;
DROP TABLE src;
