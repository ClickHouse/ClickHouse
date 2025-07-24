-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS codecTest;

-- Check error on FixedString with and without argument.
CREATE TABLE codecTest (c0 FixedString(9) CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecTest (c0 FixedString(9) CODEC(DoubleDelta(1))) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Similarly on LowCardinality(nullable)
set enable_time_time64_type=1;
CREATE TABLE codecTest (c0 LowCardinality(Nullable(Time)) CODEC(DoubleDelta)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE codecTest (c0 LowCardinality(Nullable(Time)) CODEC(DoubleDelta(2))) ENGINE = MergeTree() ORDER BY tuple();  -- { serverError BAD_ARGUMENTS }

-- Check same early fail behavior on MATERIALIZED VIEW creation
CREATE TABLE codecTest (c0 String) ENGINE = MergeTree() ORDER BY tuple();
CREATE MATERIALIZED VIEW v0 REFRESH AFTER 1 SECOND APPEND TO codecTest (c0 String CODEC(DoubleDelta(2))) EMPTY AS (SELECT 'a' AS c0); -- { serverError BAD_ARGUMENTS } 

DROP TABLE codecTest;
