-- https://github.com/ClickHouse/ClickHouse/pull/84383

DROP TABLE IF EXISTS codecTest;

-- Check error on FixedString with and without argument.
CREATE TABLE codecTest (c0 FixedString(9) CODEC(Delta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecTest (c0 FixedString(9) CODEC(Delta(1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Similarly if the column is LowCardinality
CREATE TABLE codecTest (c0 LowCardinality(FixedString(9)) CODEC(Delta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecTest (c0 LowCardinality(FixedString(9)) CODEC(Delta(2))) ENGINE = MergeTree() ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

set enable_time_time64_type=1;

-- It is intended to fail here (but because Delta does not compress).
CREATE TABLE codecTest (c0 Time CODEC(Delta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Also fail in Nullable time.
CREATE TABLE codecTest (c0 Nullable(Time) CODEC(Delta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- But not in LowCardinality(nullable)
CREATE TABLE codecTest (c0 LowCardinality(Nullable(Time)) CODEC(Delta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecTest (c0 LowCardinality(Nullable(Time)) CODEC(Delta(2))) ENGINE = MergeTree() ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

-- Check same early fail behavior on MATERIALIZED VIEW creation
CREATE TABLE codecTest (c0 String) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW v0 REFRESH AFTER 1 SECOND APPEND TO codecTest (c0 String CODEC(Delta(2))) EMPTY AS (SELECT 'a' AS c0); -- { serverError BAD_ARGUMENTS } 

DROP TABLE codecTest;
