-- Tags: no-fasttest
-- SimpleAggregateFunction cannot be inside Nullable or LowCardinality, like AggregateFunction:
-- merges apply the function only to a column whose outermost type is SimpleAggregateFunction.

CREATE TABLE t_n (k UInt32, m Nullable(SimpleAggregateFunction(sum, UInt64))) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m SimpleAggregateFunction(sum, UInt64) NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m SimpleAggregateFunction(sum, UInt64) DEFAULT NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m SimpleAggregateFunction(max, LowCardinality(String)) DEFAULT NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m LowCardinality(SimpleAggregateFunction(max, String))) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m LowCardinality(SimpleAggregateFunction(max, Nullable(String)))) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(1, 'Nullable(SimpleAggregateFunction(sum, UInt64))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE TABLE t (k UInt32, m SimpleAggregateFunction(sum, UInt64), n SimpleAggregateFunction(sum, Nullable(UInt64)), s SimpleAggregateFunction(max, LowCardinality(String))) ENGINE = AggregatingMergeTree ORDER BY k;
SET data_type_default_nullable = 1;
CREATE TABLE t_n (k UInt32 NOT NULL, m SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t ADD COLUMN a SimpleAggregateFunction(sum, UInt64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE t MODIFY COLUMN m SimpleAggregateFunction(sum, UInt64) COMMENT 'm'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET data_type_default_nullable = 0;

CREATE TABLE src (k UInt32, v Nullable(UInt64), s String) ENGINE = Memory;
INSERT INTO src VALUES (1, 10, 'b'), (1, NULL, 'a'), (1, 20, 'c');
INSERT INTO t SELECT k, v, v, s FROM src SETTINGS insert_null_as_default = 1;
INSERT INTO t SELECT k, v, v, s FROM src WHERE v = 10 SETTINGS insert_null_as_default = 1;
OPTIMIZE TABLE t FINAL;
SELECT k, m, n, s, toTypeName(m) FROM t;
SELECT count() FROM t WHERE m IN (40, 50);
SELECT -1 IN (SELECT m FROM t), 40 IN (SELECT m FROM t);
SELECT toInt64(40) IN (SELECT m FROM t) SETTINGS transform_null_in = 1;
SELECT (SELECT max(m) FROM t) AS x, toTypeName(x);
SELECT (SELECT m FROM t WHERE 0) AS x, toTypeName(x);
SELECT (SELECT max(i.m) FROM t AS i WHERE i.k = o.k) AS x, toTypeName(x) FROM t AS o;
SELECT (SELECT n FROM t) AS x, toTypeName(x), (SELECT i.n FROM t AS i WHERE i.k = o.k) AS y, toTypeName(y) FROM t AS o;
SELECT sumSimpleStateIf(toNullable(number), number > 0) AS x, toTypeName(x), hex(sumSimpleStateIfState(toNullable(number), number > 0)) FROM numbers(3);
SELECT sumSimpleStateIf(if(number < 5, NULL, toNullable(number)), number > 0) AS x, toTypeName(x), hex(sumSimpleStateIfState(if(number < 5, NULL, toNullable(number)), number > 0)) FROM numbers(3);
SELECT argMax(m, toNullable(k)) AS x, toTypeName(x), argMax(m, if(k > 5, toNullable(k), NULL)) AS y, toTypeName(y) FROM t;
SELECT (SELECT CAST(1, 'SimpleAggregateFunction(max, SimpleAggregateFunction(sum, UInt64))')) AS x, toTypeName(x);
CREATE TABLE mut (k UInt32, x Nullable(UInt64), y Nullable(UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO mut VALUES (1, 0, 0);
ALTER TABLE mut UPDATE x = (SELECT m FROM t), y = (SELECT m FROM t WHERE 0) WHERE 1 SETTINGS mutations_sync = 2, mutations_execute_subqueries_on_initiator = 1;
SELECT x, y FROM mut;
CREATE TABLE d AS t ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t, m);
SELECT count() FROM d WHERE m = 40 SETTINGS optimize_skip_unused_shards = 1;
SELECT nullIf(m, 0) FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT r.m FROM (SELECT 2 AS k) AS l LEFT JOIN t AS r ON l.k = r.k SETTINGS join_use_nulls = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
INSERT INTO FUNCTION file(currentDatabase() || '_05368.orc', 'ORC', 'a Nullable(Int128)') SELECT toInt128(number) FROM numbers(2) UNION ALL SELECT NULL SETTINGS engine_file_truncate_on_insert = 1;
SELECT arraySort(groupArray(a)), toTypeName(any(a)) FROM file(currentDatabase() || '_05368.orc', 'ORC', 'a SimpleAggregateFunction(max, Int128)') SETTINGS input_format_null_as_default = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05368.arrow', 'Arrow', 'a Nullable(Int128)') SELECT toInt128(number) FROM numbers(2) UNION ALL SELECT NULL SETTINGS engine_file_truncate_on_insert = 1;
SELECT arraySort(groupArray(a)), toTypeName(any(a)) FROM file(currentDatabase() || '_05368.arrow', 'Arrow', 'a SimpleAggregateFunction(max, Int128)') SETTINGS input_format_null_as_default = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05368_composite.arrow', 'Arrow', 'l Array(UInt64), t Tuple(a UInt64, b String), m Map(String, UInt64), u Tuple(a Int128, b String)') SELECT [1, 2], (1, 'a'), map('a', 1), (toInt128(2), 'b') SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05368_composite.arrow', 'Arrow', 'l SimpleAggregateFunction(groupArrayArray, Array(UInt64)), t SimpleAggregateFunction(anyLast, Tuple(a UInt64, b String)), m SimpleAggregateFunction(sumMap, Map(String, UInt64)), u SimpleAggregateFunction(anyLast, Tuple(Int128, String))');
INSERT INTO FUNCTION file(currentDatabase() || '_05368_opaque.arrow', 'Arrow') SELECT CAST(42::Dynamic, 'SimpleAggregateFunction(any, Dynamic)') AS d SETTINGS output_format_arrow_unsupported_types = 'binary', engine_file_truncate_on_insert = 1;
SELECT hex(CAST(d AS String)), toTypeName(d) FROM file(currentDatabase() || '_05368_opaque.arrow', 'Arrow', 'd SimpleAggregateFunction(any, Dynamic)');
CREATE TABLE v (k UInt32, m SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k;
INSERT INTO v VALUES (1, 1 + 1);
SELECT m, if(k > 0, m, m), CASE WHEN k > 0 THEN m ELSE m END, toTypeName(if(k > 0, m, m)) FROM v;
CREATE TABLE b (k UInt32, v SimpleAggregateFunction(sumWithOverflow, Bool), a SimpleAggregateFunction(any, Bool), s SimpleAggregateFunction(sumWithOverflow, SimpleAggregateFunction(any, Bool))) ENGINE = Memory;
INSERT INTO b SELECT 1, toUInt16(2), toUInt16(2), toUInt16(2);
INSERT INTO b SELECT 2, toNullable(toUInt16(2)), toNullable(toUInt16(2)), toNullable(toUInt16(2)) SETTINGS insert_null_as_default = 1;
INSERT INTO b SELECT 3, toNullable('true'), toNullable('true'), toNullable('true') SETTINGS insert_null_as_default = 1;
SELECT k, toUInt8(v), toUInt8(a), toUInt8(s), toUInt16(2) IN (SELECT v FROM b WHERE k = 1), toUInt16(2) IN (SELECT a FROM b WHERE k = 1), toUInt16(2) IN (SELECT s FROM b WHERE k = 1) FROM b ORDER BY k;
SELECT 'true' IN (SELECT CAST(1, 'SimpleAggregateFunction(sumWithOverflow, Bool)')), toFixedString('true', 4) IN (SELECT CAST(1, 'SimpleAggregateFunction(groupBitOr, Bool)')), 'true' IN (SELECT s FROM b WHERE k = 3);
SELECT tuple('true') IN (SELECT tuple(a) FROM b), tuple('true') IN (SELECT tuple(CAST(true, 'SimpleAggregateFunction(sumWithOverflow, Bool)'))), tuple('(2)') IN (SELECT tuple(CAST(tuple(2), 'SimpleAggregateFunction(any, Tuple(UInt64))'))), tuple('{"a":1}') IN (SELECT tuple(CAST('{"a":1}', 'SimpleAggregateFunction(any, JSON)'))), tuple(CAST('true' AS Variant(String, UInt64))) IN (SELECT tuple(a) FROM b);
SELECT accurateCastOrNull(tuple('true'), 'Tuple(SimpleAggregateFunction(sumWithOverflow, Bool))'), accurateCastOrNull(tuple('x'), 'Tuple(SimpleAggregateFunction(any, Bool))'), accurateCastOrNull(tuple(map('a', 1)), 'Tuple(SimpleAggregateFunction(any, JSON))'), accurateCastOrNull(tuple('x'), 'Tuple(SimpleAggregateFunction(any, JSON))');
SELECT k, (SELECT count() FROM numbers(5) WHERE number = b.v) FROM b ORDER BY k;
SELECT accurateCastOrNull(CAST([1., 2.], 'Array(Dynamic)'), 'QBit(SimpleAggregateFunction(max, Float32), 2)'), accurateCastOrNull(CAST([1., 'a'], 'Array(Variant(Float64, String))'), 'QBit(SimpleAggregateFunction(max, Float32), 2)');
SELECT CAST('{"a":"2020-01-01 00:00:00.123"}'::JSON(a DateTime64(3)) AS JSON(a SimpleAggregateFunction(any, DateTime))) SETTINGS json_use_optimized_type_conversion = 1;
SELECT arrayIntersect(a, a), arrayUnion(a, a), arraySymmetricDifference(a, a), arrayIntersect(a, [toNullable(toUInt64(1)), NULL]), arrayJaccardIndex(a, [toNullable(toUInt64(1))]) FROM (SELECT [CAST(1, 'SimpleAggregateFunction(sum, UInt64)')] AS a);
SELECT m <=> toNullable(toUInt64(40)), m IS DISTINCT FROM toNullable(toUInt64(40)) FROM t;
CREATE TABLE jn (k UInt32, x Nullable(UInt64)) ENGINE = Memory;
INSERT INTO jn VALUES (1, 40), (1, 30), (1, NULL);
CREATE TABLE ju (u UInt32) ENGINE = MergeTree ORDER BY u;
INSERT INTO ju VALUES (30), (40);
SELECT count() FROM t AS l JOIN jn AS r ON l.m = r.x SETTINGS query_plan_join_swap_table = 'false';
SELECT count() FROM t AS l JOIN jn AS r ON l.m = r.x SETTINGS query_plan_join_swap_table = 'true', enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SELECT count() FROM t AS l JOIN jn AS r ON l.m = r.x JOIN ju ON ju.u = r.x;
SELECT count() FROM t AS l, jn AS r WHERE l.m = r.x;
SELECT count() FROM t AS l JOIN jn AS r ON l.k = r.k WHERE l.m = r.x;
SELECT count() FROM t AS l JOIN jn AS r ON l.k = r.k AND l.m > r.x SETTINGS query_plan_join_swap_table = 'false';
SELECT count() FROM t AS l ASOF JOIN jn AS r ON l.k = r.k AND l.m >= r.x;
SELECT count() FROM t AS l JOIN jn AS r ON l.m <=> r.x;
SELECT arrayIntersect([tuple(m)], [tuple(toNullable(toUInt64(40)))]), tuple(m) <=> tuple(toNullable(toUInt64(40))), [m] = [toNullable(toUInt64(40))], if(k > 0, m, NULL) AS x, toTypeName(x) FROM t;
SELECT x, toTypeName(x) FROM (SELECT m AS x FROM t UNION ALL SELECT NULL) ORDER BY x;
SELECT count(), toTypeName(any(m)) FROM t JOIN (SELECT x AS m FROM jn) AS r USING (m);
WITH CAST(toDateTime('2024-01-01 00:00:00'), 'SimpleAggregateFunction(max, DateTime)') AS hit, CAST(toDateTime('2024-01-01 00:00:01'), 'SimpleAggregateFunction(max, DateTime)') AS miss, materialize(CAST([toDate('2024-01-01')], 'Array(LowCardinality(Date))')) AS lc, materialize([toDate('2024-01-01')]) AS plain
SELECT has(lc, hit), indexOf(lc, hit), countEqual(lc, hit), has(plain, hit), has(lc, miss), indexOf(lc, miss), countEqual(lc, miss), has(plain, miss) SETTINGS allow_suspicious_low_cardinality_types = 1, session_timezone = 'UTC';
CREATE TABLE c (k UInt32, v Variant(SimpleAggregateFunction(any, UInt64), String), t Nullable(Tuple(x SimpleAggregateFunction(sum, UInt64)))) ENGINE = MergeTree ORDER BY k;
INSERT INTO c SELECT 1, CAST(5, 'SimpleAggregateFunction(any, UInt64)'), tuple(42);
INSERT INTO c VALUES (2, 'x', NULL);
SELECT k, v.`SimpleAggregateFunction(any, UInt64)` AS e, toTypeName(e), t.x AS x, toTypeName(x), variantElement(v, 'SimpleAggregateFunction(any, UInt64)'), tupleElement(t, 'x') FROM c ORDER BY k;
SELECT d.`SimpleAggregateFunction(any, UInt64)` AS x, toTypeName(x), s.`SimpleAggregateFunction(any, UInt64)` FROM (SELECT arrayJoin([CAST(CAST(5, 'SimpleAggregateFunction(any, UInt64)'), 'Dynamic'), 'x'::Dynamic]) AS d, CAST(d, 'Dynamic(max_types=0)') AS s) ORDER BY x;
