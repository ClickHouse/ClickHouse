-- The prefilter's pruning is per chunk, so the stream count and the block size decide WHICH rows it
-- forwards; every assertion here is on the RESULT, which must not depend on either. Neither is pinned,
-- so the runner's own parallelism exercises that.
-- The hint is not serialized, so a serialized plan never runs the prefilter and every "fires" line would
-- read 0 (the Stress job passes serialize_query_plan=1 to the client).
SET serialize_query_plan = 0;
-- Randomized over [0,1,10,100,1000,100000]; the pass declines a bound above it.
SET query_plan_max_limit_for_top_k_optimization = 100000;

DROP TABLE IF EXISTS t_wtkp;
CREATE TABLE t_wtkp (p UInt8, o UInt8) ENGINE = Memory;
-- p=1: a tie block that ENDS exactly at the bound 3 (three 8s at rank 3) - all three must come back.
-- p=2: a tie block that STARTS at the bound 3.
-- p=3: a tie block that starts at 4, one past the bound - all three must be dropped.
INSERT INTO t_wtkp VALUES (1,10),(1,9),(1,8),(1,8),(1,8),(1,7),(2,10),(2,9),(2,7),(2,7),(3,10),(3,9),(3,8),(3,6),(3,6),(3,6);

SELECT '-- 1,2 ties at and past the bound';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 ORDER BY p, o, rk;
SELECT '-- fires';
SELECT count() FROM (EXPLAIN actions=1 SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';

SELECT '-- 3 equals, strict less and the mirrored form';
SELECT arraySort(groupArray(o)) FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp WHERE p = 1) WHERE rk = 1;
SELECT arraySort(groupArray(o)) FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp WHERE p = 1) WHERE rk < 4;
SELECT arraySort(groupArray(o)) FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp WHERE p = 1) WHERE 4 >= rk;

SELECT '-- 4 row_number dedup idiom, and 5 the smallest of two bounds';
SELECT p, o FROM (SELECT p, o, row_number() OVER (PARTITION BY p ORDER BY o DESC) AS rn FROM t_wtkp) WHERE rn <= 1 ORDER BY p;
SELECT p, count() FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk, row_number() OVER (PARTITION BY p ORDER BY o DESC) AS rn FROM t_wtkp) WHERE rk <= 5 AND rn <= 2 GROUP BY p ORDER BY p;

SELECT '-- no PARTITION BY: the whole input is one partition';
SELECT o FROM (SELECT o, rank() OVER (ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 2 ORDER BY o;

SELECT '-- 9,10 NULL partition and order keys, NULLS FIRST and NULLS LAST';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC NULLS LAST) AS rk FROM (SELECT number % 3 = 2 ? NULL : number % 3 AS p, number % 4 = 3 ? NULL : number AS o FROM numbers(24)) ) WHERE rk <= 2 ORDER BY p, o, rk;
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o ASC NULLS FIRST) AS rk FROM (SELECT number % 3 = 2 ? NULL : number % 3 AS p, number % 4 = 3 ? NULL : number AS o FROM numbers(24)) ) WHERE rk <= 2 ORDER BY p, o, rk;
SELECT '-- WITH ROLLUP, reproducing the NULL group of TPC-DS query_67';
SELECT cat, sumsales, rk FROM (SELECT cat, sumsales, rank() OVER (PARTITION BY cat ORDER BY sumsales DESC) AS rk FROM (SELECT number % 3 AS cat, number AS sub, sum(number) AS sumsales FROM numbers(30) GROUP BY cat, sub WITH ROLLUP)) WHERE rk <= 2 ORDER BY cat, sumsales, rk SETTINGS group_by_use_nulls = 1;

SELECT '-- 11 LowCardinality partition key, LowCardinality(Nullable) order key';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM (SELECT toLowCardinality(toString(number % 3)) AS p, CAST(number % 5 = 4 ? NULL : toString(number), 'LowCardinality(Nullable(String))') AS o FROM numbers(20))) WHERE rk <= 2 ORDER BY p, o, rk;

SELECT '-- 12 float keys: -0.0 vs 0.0 compare equal but hash differently, NaN, all-NaN partition, Const';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM VALUES('p Float64, o Float64', (0.0, 1), (-0.0, 2), (0.0, 3), (-0.0, 4), (nan, 5), (nan, nan), (1.0, nan), (1.0, 7))) WHERE rk <= 2 ORDER BY p, o, rk;
SELECT count(), sum(rk) FROM (SELECT rank() OVER (PARTITION BY 1 ORDER BY number DESC) AS rk FROM numbers(20)) WHERE rk <= 3;
SELECT '-- Sparse serialization';
DROP TABLE IF EXISTS t_wtkp_sparse;
CREATE TABLE t_wtkp_sparse (p UInt8, o UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO t_wtkp_sparse SELECT number % 2, if(number % 5 = 0, number, 0) FROM numbers(40);
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_sparse) WHERE rk <= 2 ORDER BY p, o, rk;

SELECT '-- 26 String partition key and Array order key';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM (SELECT toString(number % 3) AS p, [number % 4, number] AS o FROM numbers(15))) WHERE rk <= 2 ORDER BY p, o, rk;

SELECT '-- 29 chunk boundaries: a tie block straddling a chunk edge must not lose rows';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 ORDER BY p, o, rk SETTINGS max_block_size = 2;
SELECT p, count(), sum(rk) FROM (SELECT number % 7 AS p, rank() OVER (PARTITION BY number % 7 ORDER BY number % 11 DESC) AS rk FROM numbers(300)) WHERE rk <= 4 GROUP BY p ORDER BY p SETTINGS max_block_size = 8;

SELECT '-- 23 several streams: one partition split across streams';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 ORDER BY p, o, rk SETTINGS max_threads = 4, max_block_size = 2;

SELECT '-- 22 MergeTree whose PARTITION BY matches the window, so the per-partition window fires too';
DROP TABLE IF EXISTS t_wtkp_mt;
CREATE TABLE t_wtkp_mt (p UInt8, o UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY o;
INSERT INTO t_wtkp_mt SELECT number % 4, number FROM numbers(200);
SELECT p, count(), sum(rk) FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_mt) WHERE rk <= 3 GROUP BY p ORDER BY p;
SELECT '-- fires';
SELECT count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_mt) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';

SELECT '-- 25 idempotence: a plan optimized more than once carries exactly one hint';
DROP TABLE IF EXISTS t_wtkp_merge;
CREATE TABLE t_wtkp_merge (p UInt8, o UInt8) ENGINE = Merge(currentDatabase(), '^t_wtkp$');
SELECT count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_merge) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_merge) WHERE rk <= 3 ORDER BY p, o, rk;

SELECT '-- 27 the hint survives a cloned subplan (window result used as an IN set)';
SELECT count() FROM t_wtkp WHERE o IN (SELECT o FROM (SELECT o, rank() OVER (ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 2);

SELECT '-- (C) still optimized: now() is constant within the query and folded before the filter is built';
SELECT count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND now() > toDateTime('2000-01-01')) WHERE explain ILIKE '%Window top-K prefilter%';

SELECT '-- a conjunct on the PARTITION BY columns is pushed below the window, which leaves a step';
SELECT '-- between the window and its sort, so the prefilter declines: correct, only slower';
SELECT count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND p < 100) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND p < 100 ORDER BY p, o, rk;

SELECT '-- (B) the pass must DECLINE below: every count is 0';
SELECT '6 dense_rank', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, dense_rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '7 sibling aggregate', count() FROM (EXPLAIN actions=1 SELECT p, rk, s FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk, sum(o) OVER (PARTITION BY p ORDER BY o DESC) AS s FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '8 non-default frame', count() FROM (EXPLAIN actions=1 SELECT p, c FROM (SELECT p, count() OVER (PARTITION BY p ORDER BY o DESC ROWS UNBOUNDED PRECEDING) AS c FROM t_wtkp) WHERE c <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '8b rank with a non-default frame', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '13 stacked windows', count() FROM (EXPLAIN actions=1 SELECT p, rk, rk2 FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk, rank() OVER (PARTITION BY o ORDER BY p DESC) AS rk2 FROM t_wtkp) WHERE rk2 <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '15 collation', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY toString(o) DESC COLLATE 'en') AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '17 rowNumberInAllBlocks', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND rowNumberInAllBlocks() < 5) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '18 rowNumberInBlock', count() FROM (EXPLAIN actions=1 SELECT p, rn FROM (SELECT p, row_number() OVER (PARTITION BY p ORDER BY o DESC) AS rn FROM t_wtkp) WHERE rn <= 3 AND rowNumberInBlock() < 5) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '-- 19b with expression merging off, six Expression steps separate the filter from the window and';
SELECT '--     one of them computes rowNumberInAllBlocks(): the window must be the DIRECT child';
SELECT '19b not the direct child', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rk, rowNumberInAllBlocks() AS n FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp)) WHERE rk <= 3 AND n < 5 SETTINGS query_plan_merge_expressions = 0) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT p, rk, n FROM (SELECT p, rk, rowNumberInAllBlocks() AS n FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp)) WHERE rk <= 3 AND n < 5 ORDER BY p, rk, n SETTINGS query_plan_merge_expressions = 0;
SELECT '19 computing step between', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rk, rowNumberInAllBlocks() AS n FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp)) WHERE rk <= 3 AND n < 5) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '20 lambda body, rand, sleepEachRow', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND arrayExists(x -> ((x + rowNumberInAllBlocks()) < 5), [0])) WHERE explain ILIKE '%Window top-K prefilter%';
-- A lambda is folded into a `COLUMN` node, which carries neither the argument types its functions were
-- resolved with nor a way to ask them whether they throw, so it is refused even when its body is harmless.
SELECT '20b deterministic lambda body', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND arrayExists(x -> (x + o) < 1000, [1, 2])) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND arrayExists(x -> (x + o) < 1000, [1, 2]) ORDER BY p, o, rk;
-- `o + 1` is merged into the filter step, and `plus` describes no `canThrow` of its own while it can fail
-- on a value (a Decimal sum overflows), so a conjunct computing it is refused even where, as here, it
-- cannot fail. The rows must be the same as without it.
SELECT '20c a conjunct computing o + 1', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, o, o + 1 AS x, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND x > 0) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT p, o, rk FROM (SELECT p, o, o + 1 AS x, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND x > 0 ORDER BY p, o, rk;
SELECT '20 rand', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND rand() > 0) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '20 sleepEachRow', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 AND sleepEachRow(0) = 0) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '24 bound above the limit', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%' SETTINGS query_plan_max_limit_for_top_k_optimization = 2;
SELECT '30 a throwing conjunct beside the bound', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE throwIf(rk > 1) = 0 AND rk <= 1) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT '31 the setting itself declines', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3 SETTINGS query_plan_window_top_k_prefilter = 0) WHERE explain ILIKE '%Window top-K prefilter%';

SELECT '-- 30b the rows the prefilter would drop are the ones `throwIf` throws on, so the exception';
SELECT '--     the query raises today must survive the optimization';
SELECT count() FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE throwIf(rk > 1) = 0 AND rk <= 1 SETTINGS query_plan_window_top_k_prefilter = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count() FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE throwIf(rk > 1) = 0 AND rk <= 1 SETTINGS query_plan_window_top_k_prefilter = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT '-- 33 a function can be cheap and still throw on a value: `IPv4StringToNum` describes no';
SELECT '--     `canThrow` of its own, so the property falls back to the short-circuit answer, which is';
SELECT '--     `false` for it. Only an allowlist keeps the exception below alive.';
DROP TABLE IF EXISTS t_wtkp_ip;
CREATE TABLE t_wtkp_ip (p UInt8, o UInt8, s String) ENGINE = Memory;
-- The middle row is the one the prefilter would remove at `rk <= 1`, and it holds the only unparseable
-- value. A filter evaluates its conjuncts in order, each one only on the rows the previous one kept, so
-- the throwing conjunct comes FIRST: placed after `rk <= 1` it would never see that row anyway.
INSERT INTO t_wtkp_ip VALUES (1,10,'1.2.3.4'),(1,9,'not-an-ip'),(1,8,'5.6.7.8');
-- `cast_ipv4_ipv6_default_on_conversion_error` is read in the function's constructor and returns a default
-- instead of throwing, which would make all three statements vacuous. The runner does not randomize it.
SELECT '33 a cheap throwing conjunct beside the bound', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, s, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_ip) WHERE IPv4StringToNum(s) > 0 AND rk <= 1) WHERE explain ILIKE '%Window top-K prefilter%' SETTINGS cast_ipv4_ipv6_default_on_conversion_error = 0;
SELECT count() FROM (SELECT p, s, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_ip) WHERE IPv4StringToNum(s) > 0 AND rk <= 1 SETTINGS query_plan_window_top_k_prefilter = 0, cast_ipv4_ipv6_default_on_conversion_error = 0; -- { serverError CANNOT_PARSE_IPV4 }
SELECT count() FROM (SELECT p, s, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp_ip) WHERE IPv4StringToNum(s) > 0 AND rk <= 1 SETTINGS query_plan_window_top_k_prefilter = 1, cast_ipv4_ipv6_default_on_conversion_error = 0; -- { serverError CANNOT_PARSE_IPV4 }

SELECT '-- 21 an ARRAY JOIN above the window: filter push-down moves the WHERE below it, so the';
SELECT '--    prefilter is admitted and the expanded rows must be unchanged';
SELECT p, rk, a FROM (SELECT p, rk, a FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk, [1, 2] AS arr FROM t_wtkp) ARRAY JOIN arr AS a) WHERE rk <= 3 ORDER BY p, rk, a;

SELECT '-- 15b WITH FILL inside the window ORDER BY: declines';
SELECT '15b with fill', count() FROM (EXPLAIN actions=1 SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC WITH FILL) AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%Window top-K prefilter%';
SELECT p, o, rk FROM (SELECT p, o, rank() OVER (PARTITION BY p ORDER BY o DESC WITH FILL) AS rk FROM t_wtkp) WHERE rk <= 3 ORDER BY p, o, rk;

SELECT '-- 16 max_rows_to_sort: the same error with the optimization on and off';
SELECT count() FROM (SELECT rank() OVER (PARTITION BY number % 3 ORDER BY number DESC) AS rk FROM numbers(2000)) WHERE rk <= 3 SETTINGS max_rows_to_sort = 1000, query_plan_window_top_k_prefilter = 0; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT count() FROM (SELECT rank() OVER (PARTITION BY number % 3 ORDER BY number DESC) AS rk FROM numbers(2000)) WHERE rk <= 3 SETTINGS max_rows_to_sort = 1000, query_plan_window_top_k_prefilter = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT '-- the pipeline really carries the transform';
SELECT count() FROM (EXPLAIN PIPELINE SELECT p, rk FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM t_wtkp) WHERE rk <= 3) WHERE explain ILIKE '%WindowTopKPrefilterTransform%';

SELECT '-- 32 the transform really drops rows, it does not just carry them. The chunk size and the stream';
SELECT '--     count decide how much is dropped, so both are pinned on this cell alone, and `o` descends so';
SELECT '--     that each partition meets its three best rows first and every later row of the chunk is';
SELECT '--     droppable (ascending `o` under a DESC window is the shape where nothing can be dropped).';
SELECT count() FROM (SELECT p, rank() OVER (PARTITION BY p ORDER BY o DESC) AS rk FROM (SELECT number % 4 AS p, 20000 - number AS o FROM numbers(20000))) WHERE rk <= 3
SETTINGS max_threads = 1, max_block_size = 8192, log_processors_profiles = 1, log_queries = 1, log_comment = '05218_window_top_k_prefilter_pruning' FORMAT Null;
SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT '32 rows dropped', sum(output_rows) < sum(input_rows) FROM system.processors_profile_log
WHERE event_date >= yesterday() AND name = 'WindowTopKPrefilterTransform' AND query_id IN
(
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND log_comment = '05218_window_top_k_prefilter_pruning' AND type = 'QueryFinish'
);

DROP TABLE t_wtkp_ip;
DROP TABLE t_wtkp_merge;
DROP TABLE t_wtkp_mt;
DROP TABLE t_wtkp_sparse;
DROP TABLE t_wtkp;
