-- Tags: no-random-settings

SET session_timezone = 'UTC';
SET allow_experimental_parallel_reading_from_replicas = 0;

-- =====================================================
-- Setup: UInt64 key table with enough data for many granules
-- =====================================================
DROP TABLE IF EXISTS t_count_gran;
CREATE TABLE t_count_gran (k UInt64, v String) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_count_gran SELECT number, toString(number) FROM numbers(1000000);

-- =====================================================
-- 1. Basic correctness: on vs off must match
-- =====================================================
SELECT 'basic_on';
SET optimize_trivial_group_by_count_query = 1;
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran GROUP BY bucket ORDER BY bucket;

SELECT 'basic_off';
SET optimize_trivial_group_by_count_query = 0;
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran GROUP BY bucket ORDER BY bucket;

-- =====================================================
-- 2. EXPLAIN: optimization fires / does not fire
-- =====================================================
SET optimize_trivial_group_by_count_query = 1;
SELECT 'explain_on';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

SET optimize_trivial_group_by_count_query = 0;
SELECT 'explain_off';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 3. Must NOT fire with WHERE
-- =====================================================
SET optimize_trivial_group_by_count_query = 1;
SELECT 'no_where_non_pk';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran WHERE v != '' GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 4. Must NOT fire with PREWHERE on non-PK column
-- =====================================================
SELECT 'no_prewhere_non_pk';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran PREWHERE v != '' GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 4b. WHERE on PK column: correctness (WHERE gets pushed to PREWHERE automatically)
-- =====================================================
SELECT 'where_pk_on';
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran WHERE k >= 200000 AND k < 800000 GROUP BY bucket ORDER BY bucket;
SET optimize_trivial_group_by_count_query = 0;
SELECT 'where_pk_off';
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran WHERE k >= 200000 AND k < 800000 GROUP BY bucket ORDER BY bucket;
SET optimize_trivial_group_by_count_query = 1;

-- =====================================================
-- 4c. PREWHERE on PK column: should trigger
-- =====================================================
SELECT 'prewhere_pk_on';
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran PREWHERE k >= 200000 AND k < 800000 GROUP BY bucket ORDER BY bucket;
SET optimize_trivial_group_by_count_query = 0;
SELECT 'prewhere_pk_off';
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran PREWHERE k >= 200000 AND k < 800000 GROUP BY bucket ORDER BY bucket;
SET optimize_trivial_group_by_count_query = 1;
SELECT 'prewhere_pk_explain';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran PREWHERE k >= 200000 AND k < 800000 GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 4d. PREWHERE on PK but not a range condition: must NOT fire
-- =====================================================
SELECT 'no_prewhere_non_range';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran PREWHERE k % 2 = 0 GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 5. Must NOT fire with FINAL
-- =====================================================
DROP TABLE IF EXISTS t_rmt;
CREATE TABLE t_rmt (k UInt64, v UInt64) ENGINE = ReplacingMergeTree() ORDER BY k;
INSERT INTO t_rmt SELECT number, number FROM numbers(1000);
SELECT 'no_final';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100) AS bucket, count() FROM t_rmt FINAL GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';
DROP TABLE t_rmt;

-- =====================================================
-- 6. Must NOT fire with SAMPLE
-- =====================================================
DROP TABLE IF EXISTS t_sample;
CREATE TABLE t_sample (k UInt64, v UInt64) ENGINE = MergeTree() ORDER BY k SAMPLE BY k;
INSERT INTO t_sample SELECT number, number FROM numbers(1000);
SELECT 'no_sample';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100) AS bucket, count() FROM t_sample SAMPLE 0.5 GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';
DROP TABLE t_sample;

-- =====================================================
-- 7. Must NOT fire with count(col) — only count(*)
-- =====================================================
SELECT 'no_count_col';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count(v) FROM t_count_gran GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 8. Must NOT fire with multiple GROUP BY keys
-- =====================================================
SELECT 'no_multi_key';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS b1, intDiv(k, 10000) AS b2, count() FROM t_count_gran GROUP BY b1, b2) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 9. Must NOT fire with multiple aggregates
-- =====================================================
SELECT 'no_multi_agg';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(k, 100000) AS bucket, count(), min(k) FROM t_count_gran GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 10. Must NOT fire with non-monotonic function
-- =====================================================
SELECT 'no_non_monotonic';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT abs(k - 500000) AS bucket, count() FROM t_count_gran GROUP BY bucket) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- =====================================================
-- 11. Multiple parts
-- =====================================================
INSERT INTO t_count_gran SELECT number + 1000000, toString(number) FROM numbers(500000);

SELECT 'multi_part_on';
SET optimize_trivial_group_by_count_query = 1;
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran GROUP BY bucket ORDER BY bucket;

SELECT 'multi_part_off';
SET optimize_trivial_group_by_count_query = 0;
SELECT intDiv(k, 100000) AS bucket, count() FROM t_count_gran GROUP BY bucket ORDER BY bucket;

DROP TABLE t_count_gran;

-- =====================================================
-- 12. DateTime with toStartOfInterval — cross-boundary granules
-- =====================================================
DROP TABLE IF EXISTS t_count_ts;
CREATE TABLE t_count_ts (ts DateTime, v UInt64) ENGINE = MergeTree() ORDER BY ts;
INSERT INTO t_count_ts SELECT toDateTime('2024-01-01') + number, number FROM numbers(864000);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'interval_on';
SELECT toStartOfInterval(ts, INTERVAL 6 HOUR) AS bucket, count() FROM t_count_ts GROUP BY bucket ORDER BY bucket;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'interval_off';
SELECT toStartOfInterval(ts, INTERVAL 6 HOUR) AS bucket, count() FROM t_count_ts GROUP BY bucket ORDER BY bucket;

DROP TABLE t_count_ts;

-- =====================================================
-- 13. HAVING works correctly
-- =====================================================
DROP TABLE IF EXISTS t_having;
CREATE TABLE t_having (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_having SELECT number FROM numbers(100);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'having_on';
SELECT intDiv(k, 10) AS bucket, count() AS c FROM t_having GROUP BY bucket HAVING c > 9 ORDER BY bucket;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'having_off';
SELECT intDiv(k, 10) AS bucket, count() AS c FROM t_having GROUP BY bucket HAVING c > 9 ORDER BY bucket;

DROP TABLE t_having;

-- =====================================================
-- 14. ORDER BY DESC
-- =====================================================
DROP TABLE IF EXISTS t_desc;
CREATE TABLE t_desc (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_desc SELECT number FROM numbers(50);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'desc_on';
SELECT intDiv(k, 10) AS bucket, count() FROM t_desc GROUP BY bucket ORDER BY bucket DESC;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'desc_off';
SELECT intDiv(k, 10) AS bucket, count() FROM t_desc GROUP BY bucket ORDER BY bucket DESC;

DROP TABLE t_desc;

-- =====================================================
-- 15. Identity: GROUP BY pk itself (no function wrapping)
-- =====================================================
DROP TABLE IF EXISTS t_identity;
CREATE TABLE t_identity (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_identity VALUES (1), (1), (2), (3), (3), (3);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'identity_on';
SELECT k, count() FROM t_identity GROUP BY k ORDER BY k;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'identity_off';
SELECT k, count() FROM t_identity GROUP BY k ORDER BY k;

DROP TABLE t_identity;

-- =====================================================
-- 16. Empty table
-- =====================================================
DROP TABLE IF EXISTS t_count_empty;
CREATE TABLE t_count_empty (k UInt64) ENGINE = MergeTree() ORDER BY k;
SET optimize_trivial_group_by_count_query = 1;
SELECT 'empty';
SELECT intDiv(k, 10) AS bucket, count() FROM t_count_empty GROUP BY bucket ORDER BY bucket;
DROP TABLE t_count_empty;

-- =====================================================
-- 17. Single row
-- =====================================================
DROP TABLE IF EXISTS t_count_one;
CREATE TABLE t_count_one (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_count_one VALUES (42);
SELECT 'single_on';
SELECT intDiv(k, 10) AS bucket, count() FROM t_count_one GROUP BY bucket ORDER BY bucket;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'single_off';
SELECT intDiv(k, 10) AS bucket, count() FROM t_count_one GROUP BY bucket ORDER BY bucket;
DROP TABLE t_count_one;

-- =====================================================
-- 18. Small table: 1 granule spanning multiple buckets
-- =====================================================
DROP TABLE IF EXISTS t_small;
CREATE TABLE t_small (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_small SELECT number FROM numbers(30);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'small_on';
SELECT intDiv(k, 10) AS bucket, count() FROM t_small GROUP BY bucket ORDER BY bucket;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'small_off';
SELECT intDiv(k, 10) AS bucket, count() FROM t_small GROUP BY bucket ORDER BY bucket;

DROP TABLE t_small;

-- =====================================================
-- 19. Negative monotonic: negate(k)
-- =====================================================
DROP TABLE IF EXISTS t_neg;
CREATE TABLE t_neg (k Int64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_neg SELECT number FROM numbers(100);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'negate_on';
SELECT intDiv(negate(k), 10) AS bucket, count() FROM t_neg GROUP BY bucket ORDER BY bucket;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'negate_off';
SELECT intDiv(negate(k), 10) AS bucket, count() FROM t_neg GROUP BY bucket ORDER BY bucket;

DROP TABLE t_neg;

-- =====================================================
-- 21. Multi-key GROUP BY: ORDER BY (a, b), GROUP BY f(a), g(b)
-- =====================================================
DROP TABLE IF EXISTS t_multi_key;
CREATE TABLE t_multi_key (a UInt64, b UInt64, v String) ENGINE = MergeTree() ORDER BY (a, b);
INSERT INTO t_multi_key SELECT intDiv(number, 100), number % 100, toString(number) FROM numbers(10000);

SET optimize_trivial_group_by_count_query = 1;
SELECT 'multi_key_on';
SELECT intDiv(a, 10) AS ba, intDiv(b, 50) AS bb, count() FROM t_multi_key GROUP BY ba, bb ORDER BY ba, bb;

SET optimize_trivial_group_by_count_query = 0;
SELECT 'multi_key_off';
SELECT intDiv(a, 10) AS ba, intDiv(b, 50) AS bb, count() FROM t_multi_key GROUP BY ba, bb ORDER BY ba, bb;

SET optimize_trivial_group_by_count_query = 1;
SELECT 'multi_key_explain';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(a, 10) AS ba, intDiv(b, 50) AS bb, count() FROM t_multi_key GROUP BY ba, bb) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- Must NOT fire: GROUP BY only second PK column (skips first)
SELECT 'no_skip_pk_prefix';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(b, 50) AS bb, count() FROM t_multi_key GROUP BY bb) WHERE explain LIKE '%ReadFromCountByGranularity%';

-- Must NOT fire: GROUP BY key order doesn't match PK prefix
SELECT 'no_wrong_key_order';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT intDiv(b, 50) AS bb, intDiv(a, 10) AS ba, count() FROM t_multi_key GROUP BY bb, ba) WHERE explain LIKE '%ReadFromCountByGranularity%';

DROP TABLE t_multi_key;

-- =====================================================
-- 23. ORDER BY expression (not identifier): must NOT fire
-- =====================================================
DROP TABLE IF EXISTS t_pk_expr;
CREATE TABLE t_pk_expr (ts DateTime, v UInt64) ENGINE = MergeTree() ORDER BY toStartOfHour(ts);
INSERT INTO t_pk_expr SELECT toDateTime('2024-01-01') + number, number FROM numbers(100);
SET optimize_trivial_group_by_count_query = 1;
SELECT 'no_pk_expression';
SELECT count() > 0 FROM (EXPLAIN description=0 SELECT toStartOfHour(ts) AS hour, count() FROM t_pk_expr GROUP BY hour) WHERE explain LIKE '%ReadFromCountByGranularity%';
DROP TABLE t_pk_expr;
