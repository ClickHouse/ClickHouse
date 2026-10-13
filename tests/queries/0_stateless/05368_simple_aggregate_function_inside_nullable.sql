-- SimpleAggregateFunction cannot be inside Nullable or LowCardinality, like AggregateFunction:
-- merges apply the function only to a column whose outermost type is SimpleAggregateFunction.

CREATE TABLE t_n (k UInt32, m Nullable(SimpleAggregateFunction(sum, UInt64))) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m SimpleAggregateFunction(sum, UInt64) NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m SimpleAggregateFunction(sum, UInt64) DEFAULT NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m SimpleAggregateFunction(max, LowCardinality(String)) DEFAULT NULL) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m LowCardinality(SimpleAggregateFunction(max, String))) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_n (k UInt32, m LowCardinality(SimpleAggregateFunction(max, Nullable(String)))) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET data_type_default_nullable = 1;
CREATE TABLE t_n (k UInt32 NOT NULL, m SimpleAggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY k; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
CREATE TABLE t_nn (k UInt32 NOT NULL, m NOT NULL DEFAULT CAST(0, 'SimpleAggregateFunction(sum, UInt64)')) ENGINE = AggregatingMergeTree ORDER BY k;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_nn' AND name = 'm';
SET data_type_default_nullable = 0;
SELECT CAST(1, 'Nullable(SimpleAggregateFunction(sum, UInt64))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE TABLE t (k UInt32, m SimpleAggregateFunction(sum, UInt64), n SimpleAggregateFunction(sum, Nullable(UInt64)), s SimpleAggregateFunction(max, LowCardinality(String))) ENGINE = AggregatingMergeTree ORDER BY k;
ALTER TABLE t ADD COLUMN a SimpleAggregateFunction(sum, UInt64) NULL; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SET data_type_default_nullable = 1;
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

SELECT nullIf(m, 0) FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT nullIf(CAST(m AS UInt64), 0) FROM t;
SELECT sumSimpleStateIf(toNullable(number), number > 0) AS x, toTypeName(x), hex(sumSimpleStateIfState(toNullable(number), number > 0)) FROM numbers(3);

SELECT m, toTypeName(m), toColumnTypeName(m), count() FROM t GROUP BY ROLLUP(m) ORDER BY m SETTINGS group_by_use_nulls = 1;
SELECT m, toTypeName(m), toColumnTypeName(m), count() FROM t GROUP BY GROUPING SETS ((m), ()) ORDER BY m SETTINGS group_by_use_nulls = 1;
SELECT c, toTypeName(c), toColumnTypeName(c) FROM numbers(1) GROUP BY ROLLUP(CAST(5, 'SimpleAggregateFunction(sum, UInt64)') AS c) ORDER BY c SETTINGS group_by_use_nulls = 1;
SELECT r.m, toTypeName(r.m), toColumnTypeName(r.m) FROM (SELECT 2 AS k) AS l LEFT JOIN t AS r ON l.k = r.k SETTINGS join_use_nulls = 1;

CREATE TABLE j (k UInt32, m SimpleAggregateFunction(sum, UInt64)) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO j VALUES (1, 10);
SELECT k, m, toTypeName(m), toColumnTypeName(m) FROM j;
SELECT l.k, m, toTypeName(m), toColumnTypeName(m) FROM (SELECT number::UInt32 AS k FROM numbers(2)) AS l ANY LEFT JOIN j USING (k) ORDER BY l.k SETTINGS join_use_nulls = 1;

CREATE TABLE tn (k UInt32, t Nullable(Tuple(a SimpleAggregateFunction(sum, UInt64)))) ENGINE = Memory;
INSERT INTO tn VALUES (1, tuple(7)), (2, NULL);
SELECT t.a, toTypeName(t.a), toColumnTypeName(t.a) FROM tn ORDER BY k;
SELECT tupleElement(t, 'a') AS x, toTypeName(x), toColumnTypeName(x) FROM tn ORDER BY k;
CREATE TABLE tv (k UInt32, v Variant(SimpleAggregateFunction(sum, UInt64), Tuple(a SimpleAggregateFunction(sum, UInt64)), String)) ENGINE = Memory;
INSERT INTO tv VALUES (1, 3::SimpleAggregateFunction(sum, UInt64)), (2, tuple(4)::Tuple(a SimpleAggregateFunction(sum, UInt64))), (3, 'x');
SELECT v.`SimpleAggregateFunction(sum, UInt64)` AS x, toTypeName(x), toColumnTypeName(x), v.`Tuple(a SimpleAggregateFunction(sum, UInt64))`.a AS y, toTypeName(y), toColumnTypeName(y) FROM tv ORDER BY k;
SELECT arrayElementOrNull(CAST([1, 2], 'Array(SimpleAggregateFunction(sum, UInt64))'), 5) AS x, toTypeName(x), toColumnTypeName(x);
SELECT arrayElementOrNull(CAST(map('a', 1), 'Map(String, SimpleAggregateFunction(sum, UInt64))'), 'b') AS x, toTypeName(x), toColumnTypeName(x);
CREATE TABLE td (k UInt32, d Dynamic, s Dynamic(max_types = 0)) ENGINE = Memory;
INSERT INTO td SELECT 1, CAST(tuple(4), 'Tuple(a SimpleAggregateFunction(sum, UInt64))'), CAST(tuple(4), 'Tuple(a SimpleAggregateFunction(sum, UInt64))');
INSERT INTO td SELECT 2, 'x', 'x';
SELECT k, d.`Tuple(a SimpleAggregateFunction(sum, UInt64))`.a AS x, toTypeName(x), toColumnTypeName(x), s.`Tuple(a SimpleAggregateFunction(sum, UInt64))`.a AS y, toTypeName(y), toColumnTypeName(y) FROM td ORDER BY k;
SELECT arrayElementOrNull(CAST([1, 2, 3, 4], 'QBit(SimpleAggregateFunction(max, Float32), 4)'), 5) AS x, toTypeName(x), toColumnTypeName(x);
SELECT arrayElementOrNull(CAST([1, 2, 3, 4], 'QBit(SimpleAggregateFunction(max, Float32), 4)'), [1, 5]) AS x, toTypeName(x), toColumnTypeName(x);
CREATE TABLE tq (k UInt32, q Nullable(QBit(SimpleAggregateFunction(max, Float32), 4))) ENGINE = Memory;
INSERT INTO tq VALUES (0, [1, 2, 3, 4]), (1, NULL), (2, [1, 2, 3, 4]);
SELECT k, arrayElementOrNull(q, k + 3) AS x, toTypeName(x), toColumnTypeName(x), arrayElementOrNull(q, [1, 5]) AS y, toTypeName(y), toColumnTypeName(y) FROM tq ORDER BY k;
