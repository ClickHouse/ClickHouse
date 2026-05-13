-- The `count_distinct_optimization` rewrite must NOT apply to numeric-like
-- columns. The `uniqExact` aggregator has highly tuned open-addressing hash
-- tables for fixed-size numeric values, so the GROUP BY rewrite adds overhead
-- (an extra block-materialization stage in the pipeline) without compensating
-- savings. For `String`, `FixedString` and other variable-size types the
-- rewrite still wins, because the specialized `GROUP BY` hash tables beat the
-- generic ones used inside `uniqExact`.

SET enable_analyzer = 1;
SET count_distinct_optimization = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_num_04218;
DROP TABLE IF EXISTS t_str_04218;
DROP TABLE IF EXISTS t_dt_04218;
DROP TABLE IF EXISTS t_decimal_04218;
DROP TABLE IF EXISTS t_uuid_04218;
DROP TABLE IF EXISTS t_enum_04218;
DROP TABLE IF EXISTS t_ipv4_04218;
DROP TABLE IF EXISTS t_lc_str_04218;
DROP TABLE IF EXISTS t_lc_num_04218;
DROP TABLE IF EXISTS t_nullable_num_04218;
DROP TABLE IF EXISTS t_lc_nullable_num_04218;

CREATE TABLE t_num_04218             (x UInt64)                              ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_str_04218             (x String)                              ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dt_04218              (x DateTime)                            ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_decimal_04218         (x Decimal(18, 2))                      ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_uuid_04218            (x UUID)                                ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_enum_04218            (x Enum('a' = 1, 'b' = 2))              ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_ipv4_04218            (x IPv4)                                ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_lc_str_04218          (x LowCardinality(String))              ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_lc_num_04218          (x LowCardinality(UInt32))              ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_nullable_num_04218    (x Nullable(UInt64))                    ENGINE = MergeTree ORDER BY tuple() SETTINGS allow_nullable_key = 1;
CREATE TABLE t_lc_nullable_num_04218 (x LowCardinality(Nullable(UInt32)))    ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_num_04218             VALUES (1), (1), (2);
INSERT INTO t_str_04218             VALUES ('a'), ('a'), ('b');
INSERT INTO t_dt_04218              VALUES (1), (1), (2);
INSERT INTO t_decimal_04218         VALUES (1), (1), (2);
INSERT INTO t_uuid_04218            VALUES ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0000-0000-000000000001'), ('00000000-0000-0000-0000-000000000002');
INSERT INTO t_enum_04218            VALUES ('a'), ('a'), ('b');
INSERT INTO t_ipv4_04218            VALUES ('1.2.3.4'), ('1.2.3.4'), ('5.6.7.8');
INSERT INTO t_lc_str_04218          VALUES ('a'), ('a'), ('b');
INSERT INTO t_lc_num_04218          VALUES (1), (1), (2);
INSERT INTO t_nullable_num_04218    VALUES (1), (1), (2);
INSERT INTO t_lc_nullable_num_04218 VALUES (1), (1), (2);

-- For numeric columns, the rewrite is skipped: `function_name: uniqExact` stays.
SELECT 'numeric: UInt64',             sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_num_04218);
SELECT 'numeric: DateTime',           sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_dt_04218);
SELECT 'numeric: Decimal',            sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_decimal_04218);
SELECT 'numeric: Enum',               sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_enum_04218);
SELECT 'numeric: IPv4',               sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_ipv4_04218);
SELECT 'numeric: LC(UInt32)',         sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_lc_num_04218);
-- `DataTypeNullable` and `DataTypeLowCardinality` over `DataTypeNullable` do not
-- delegate `isValueRepresentedByNumber` to their nested type, so the gate must
-- strip those wrappers first.
SELECT 'numeric: Nullable(UInt64)',   sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_nullable_num_04218);
SELECT 'numeric: LC(Nullable(U32))',  sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_lc_nullable_num_04218);

-- For variable-size / non-numeric columns, the rewrite is applied: `function_name: count` appears.
SELECT 'non-numeric: String',     sum(countSubstrings(explain, 'function_name: count')) > 0 AS rewrote FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_str_04218);
SELECT 'non-numeric: UUID',       sum(countSubstrings(explain, 'function_name: count')) > 0 AS rewrote FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_uuid_04218);
SELECT 'non-numeric: LC(String)', sum(countSubstrings(explain, 'function_name: count')) > 0 AS rewrote FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT x) FROM t_lc_str_04218);

-- Sanity check: results are correct regardless of whether the rewrite happens.
SELECT 'UInt64',             count(DISTINCT x) FROM t_num_04218;
SELECT 'String',             count(DISTINCT x) FROM t_str_04218;
SELECT 'DateTime',           count(DISTINCT x) FROM t_dt_04218;
SELECT 'Decimal',            count(DISTINCT x) FROM t_decimal_04218;
SELECT 'UUID',               count(DISTINCT x) FROM t_uuid_04218;
SELECT 'Enum',               count(DISTINCT x) FROM t_enum_04218;
SELECT 'IPv4',               count(DISTINCT x) FROM t_ipv4_04218;
SELECT 'LC(String)',         count(DISTINCT x) FROM t_lc_str_04218;
SELECT 'LC(UInt32)',         count(DISTINCT x) FROM t_lc_num_04218;
SELECT 'Nullable(UInt64)',   count(DISTINCT x) FROM t_nullable_num_04218;
SELECT 'LC(Nullable(U32))',  count(DISTINCT x) FROM t_lc_nullable_num_04218;

DROP TABLE t_num_04218;
DROP TABLE t_str_04218;
DROP TABLE t_dt_04218;
DROP TABLE t_decimal_04218;
DROP TABLE t_uuid_04218;
DROP TABLE t_enum_04218;
DROP TABLE t_ipv4_04218;
DROP TABLE t_lc_str_04218;
DROP TABLE t_lc_num_04218;
DROP TABLE t_nullable_num_04218;
DROP TABLE t_lc_nullable_num_04218;
