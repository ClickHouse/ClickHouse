-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

SET explain_query_plan_default = 'legacy';
-- The cost heuristic needs partitions >= max_threads / 2, and the preservation arms below have
-- eight balanced partitions. The correctness arms use the force_* settings instead.
SET max_threads = 8;
SET enable_parallel_replicas = 0;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;
SET optimize_use_implicit_projections = 0;
SET allow_suspicious_low_cardinality_types = 1;

-- { echo }

-- ---------------------------------------------------------------------------
-- Correctness: per-partition evaluation must agree with the merged evaluation. Each arm
-- prints the forced count and then the merged count; they must be equal. The two settings
-- apply to the outer query, so they reach the aggregation under test.
-- ---------------------------------------------------------------------------

-- Date + INTERVAL MONTH collapses the 29th, 30th and 31st of a month into one key
DROP TABLE IF EXISTS t_month;
CREATE TABLE t_month (d Date, x UInt32) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_month SELECT toDate(concat(toString(2001 + intDiv(number, 300)), '-01-', toString(29 + (intDiv(number, 100) % 3)))) AS d, number FROM numbers_mt(5100) WHERE toYear(d) NOT IN (2004, 2008, 2012, 2016, 2020);
SELECT count() FROM (SELECT d + INTERVAL 1 MONTH AS k, count() FROM t_month GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT d + INTERVAL 1 MONTH AS k, count() FROM t_month GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
SELECT count() FROM (SELECT DISTINCT d + INTERVAL 1 MONTH AS k FROM t_month) SETTINGS force_distinct_partitions_independently = 1;
SELECT count() FROM (SELECT DISTINCT d + INTERVAL 1 MONTH AS k FROM t_month) SETTINGS allow_distinct_partitions_independently = 0;
SELECT count() FROM (SELECT d + INTERVAL 1 MONTH AS k FROM t_month LIMIT 1 BY k) SETTINGS allow_limit_by_partitions_independently = 1;
SELECT count() FROM (SELECT d + INTERVAL 1 MONTH AS k FROM t_month LIMIT 1 BY k) SETTINGS allow_limit_by_partitions_independently = 0;
DROP TABLE t_month;

-- the minus direction collapses the same way, going back into a shorter month
DROP TABLE IF EXISTS t_month_minus;
CREATE TABLE t_month_minus (d Date, x UInt32) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_month_minus SELECT toDate(concat(toString(2001 + intDiv(number, 300)), '-03-', toString(29 + (intDiv(number, 100) % 3)))) AS d, number FROM numbers_mt(5100) WHERE toYear(d) NOT IN (2004, 2008, 2012, 2016, 2020);
SELECT count() FROM (SELECT d - INTERVAL 1 MONTH AS k, count() FROM t_month_minus GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT d - INTERVAL 1 MONTH AS k, count() FROM t_month_minus GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE t_month_minus;

-- DateTime + INTERVAL DAY collapses across a DST spring-forward, so the interval kind alone
-- does not decide safety
DROP TABLE IF EXISTS t_dst;
CREATE TABLE t_dst (t DateTime('Europe/Moscow'), x UInt32) ENGINE = MergeTree ORDER BY t PARTITION BY t;
INSERT INTO t_dst SELECT toDateTime('2010-03-27 00:00:00', 'Europe/Moscow') + (intDiv(number, 100) * 1800) AS t, number FROM numbers_mt(600);
SELECT count() FROM (SELECT t + INTERVAL 1 DAY AS k, count() FROM t_dst GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT t + INTERVAL 1 DAY AS k, count() FROM t_dst GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE t_dst;

-- a Float64 addend collapses by mantissa rounding; a float column reaches the optimizer
-- through a non-float partition key
DROP TABLE IF EXISTS t_float;
CREATE TABLE t_float (f Float64, x UInt32) ENGINE = MergeTree ORDER BY f PARTITION BY toUInt64(f - 1e16);
INSERT INTO t_float SELECT 1e16 + intDiv(number, 100) AS f, number FROM numbers_mt(600);
SELECT count() FROM (SELECT f + 1.0 AS k, count() FROM t_float GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT f + 1.0 AS k, count() FROM t_float GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE t_float;

-- a constant of a narrower date type narrows the result: every multiple of 65536 maps to
-- 1970-01-01. A NULL constant maps every value to NULL.
DROP TABLE IF EXISTS t_narrow;
CREATE TABLE t_narrow (x UInt32, v UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY x;
INSERT INTO t_narrow SELECT intDiv(number, 100) * 65536 AS x, number FROM numbers_mt(600);
SELECT count() FROM (SELECT x + toDate(0) AS k, count() FROM t_narrow GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT x + toDate(0) AS k, count() FROM t_narrow GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
SELECT count() FROM (SELECT DISTINCT x + CAST(NULL, 'Nullable(UInt32)') AS k FROM t_narrow) SETTINGS force_distinct_partitions_independently = 1;
SELECT count() FROM (SELECT DISTINCT x + CAST(NULL, 'Nullable(UInt32)') AS k FROM t_narrow) SETTINGS allow_distinct_partitions_independently = 0;
DROP TABLE t_narrow;

-- a Decimal constant rescales the varying operand: every multiple of 2^32 maps to one
-- Decimal(9, 1)
DROP TABLE IF EXISTS t_decimal;
CREATE TABLE t_decimal (x UInt64, v UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY x;
INSERT INTO t_decimal SELECT intDiv(number, 100) * 4294967296 AS x, number FROM numbers_mt(400);
SELECT count() FROM (SELECT x + toDecimal32(0, 1) AS k, count() FROM t_decimal GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT x + toDecimal32(0, 1) AS k, count() FROM t_decimal GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE t_decimal;

-- two varying operands are not injective either: many pairs share one sum
DROP TABLE IF EXISTS t_two_cols;
CREATE TABLE t_two_cols (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY x;
INSERT INTO t_two_cols SELECT intDiv(number, 100) AS x, number % 100 AS y FROM numbers_mt(1000);
SELECT count() FROM (SELECT x + y AS k, count() FROM t_two_cols GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT x + y AS k, count() FROM t_two_cols GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE t_two_cols;

-- ---------------------------------------------------------------------------
-- Preservation: integer arithmetic with an integer constant stays injective, so the
-- optimization must keep firing. A correctness-only test would not notice this. Each arm
-- prints two plan lines; an arm printing nothing has lost the optimization.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS t_int;
CREATE TABLE t_int (a UInt32) ENGINE = MergeTree ORDER BY a PARTITION BY intDiv(a, 2) * 2 + 1;
INSERT INTO t_int SELECT number FROM numbers_mt(32);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT intDiv(a, 2) + 1 AS a1 FROM t_int SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_int;

-- wide integers are exact, so the gate must not be narrowed to native widths
DROP TABLE IF EXISTS t_int128;
CREATE TABLE t_int128 (a Int128) ENGINE = MergeTree ORDER BY a PARTITION BY a % 8;
INSERT INTO t_int128 SELECT number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a + 7 AS a1 FROM t_int128 SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_int128;

DROP TABLE IF EXISTS t_uint256;
CREATE TABLE t_uint256 (b UInt256) ENGINE = MergeTree ORDER BY b PARTITION BY b % 8;
INSERT INTO t_uint256 SELECT number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT b + 7 AS b1 FROM t_uint256 SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_uint256;

-- both type-wrapper orders must be stripped before the integer test. Contrast with the NULL
-- constant arm above: a Nullable column keeps the optimization, a NULL constant loses it.
DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable (n Nullable(UInt32)) ENGINE = MergeTree ORDER BY n PARTITION BY n % 8 SETTINGS allow_nullable_key = 1;
INSERT INTO t_nullable SELECT number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT n + 1 AS n1 FROM t_nullable SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_nullable;

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc (l LowCardinality(UInt32)) ENGINE = MergeTree ORDER BY l PARTITION BY l % 8;
INSERT INTO t_lc SELECT number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT l + 1 AS l1 FROM t_lc SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_lc;

DROP TABLE IF EXISTS t_lc_nullable;
CREATE TABLE t_lc_nullable (ln LowCardinality(Nullable(UInt32))) ENGINE = MergeTree ORDER BY ln PARTITION BY ln % 8 SETTINGS allow_nullable_key = 1;
INSERT INTO t_lc_nullable SELECT number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT ln + 1 AS ln1 FROM t_lc_nullable SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_lc_nullable;

-- ---------------------------------------------------------------------------
-- Accepted narrowings. These three expressions are injective, but every type they would
-- admit also admits a non-injective case above that cannot be told apart: an Interval on
-- Date is not separable from one on DateTime by kind, a Date operand re-admits the
-- narrowing constant, and a Decimal operand re-admits the rescaling constant. Each arm
-- prints nothing; the bare-key arm after it is the control showing the fixture does reach
-- the optimization.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d PARTITION BY toDayOfWeek(d);
INSERT INTO t_date SELECT toDate('2001-01-01') + number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT d + INTERVAL 1 DAY AS d1 FROM t_date SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT d + 1 AS d1 FROM t_date SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT toDayOfWeek(d) AS d1 FROM t_date SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_date;

DROP TABLE IF EXISTS t_dec;
CREATE TABLE t_dec (dec Decimal64(2)) ENGINE = MergeTree ORDER BY dec PARTITION BY toUInt32(dec) % 8;
INSERT INTO t_dec SELECT number FROM numbers_mt(200);
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT dec + 1 AS dec1 FROM t_dec SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT toUInt32(dec) AS dec1 FROM t_dec SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Read each partition through separate port%';
DROP TABLE t_dec;

-- ---------------------------------------------------------------------------
-- A GROUP BY key that is now an injective function of constants unwraps to an empty key
-- list, and the query must still aggregate.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS t_const_key;
CREATE TABLE t_const_key (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_const_key SELECT number FROM numbers(10);
SELECT count() FROM (SELECT 1 FROM t_const_key GROUP BY materialize(1) + 1);
SELECT count() FROM (SELECT 1 FROM t_const_key GROUP BY materialize(1) + 1, k);
SELECT count() FROM (SELECT k FROM t_const_key ORDER BY k LIMIT 1 BY materialize(1) + 1);
DROP TABLE t_const_key;
