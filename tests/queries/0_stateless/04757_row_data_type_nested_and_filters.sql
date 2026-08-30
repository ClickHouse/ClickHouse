-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_nested;
DROP TABLE IF EXISTS row_prewhere;
DROP TABLE IF EXISTS row_double_wrap;

-- A Row(...) nested in another type must have a serialization hash, otherwise
-- the enclosing serialization throws "Hash is not set for serialization".
CREATE TABLE row_nested (
    id UInt64,
    a String,
    b UInt32,
    c String,
    combined Array(Row(a String, b UInt32, c String)) ALIAS [(a, b, c)]
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_nested (id, a, b, c) VALUES (1, 'alpha', 10, 'x');
SELECT combined FROM row_nested ORDER BY id;

DROP TABLE row_nested;

-- A column consumed by PREWHERE must keep being read directly: the wrapper
-- rewrite does not touch the PREWHERE DAG.
CREATE TABLE row_prewhere (
    id UInt64,
    a UInt64,
    b UInt64,
    c UInt64,
    combined Row(a UInt64, b UInt64, c UInt64) MATERIALIZED tuple(a, b, c)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_prewhere (id, a, b, c) SELECT number, number, number * 2, number * 3 FROM numbers(100);

SELECT b, c FROM row_prewhere PREWHERE a = 7 SETTINGS query_plan_use_row_wrappers = 1;
SELECT b, c FROM row_prewhere PREWHERE a = 7 SETTINGS query_plan_use_row_wrappers = 0;
SELECT sum(b) FROM row_prewhere PREWHERE (a > 10) AND (c < 150) SETTINGS query_plan_use_row_wrappers = 1;
SELECT sum(b) FROM row_prewhere PREWHERE (a > 10) AND (c < 150) SETTINGS query_plan_use_row_wrappers = 0;

-- Casting between two Row(...) types works: both sides lower to a Tuple cast.
SELECT CAST(combined AS Row(a UInt64, b UInt64)) FROM row_prewhere WHERE id = 3;

DROP TABLE row_prewhere;

-- Wrapping the same source column twice is a schema error, reported by CREATE
-- TABLE rather than lazily by the read optimizer.
CREATE TABLE row_double_wrap (
    id UInt64,
    a String,
    b UInt32,
    first Row(a String, b UInt32) MATERIALIZED tuple(a, b),
    second Row(a String) MATERIALIZED tuple(a)
) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
