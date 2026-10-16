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
SELECT (SELECT max(i.m) FROM t AS i WHERE i.k = o.k) AS x, toTypeName(x) FROM t AS o;
SELECT (SELECT n FROM t) AS x, toTypeName(x), (SELECT i.n FROM t AS i WHERE i.k = o.k) AS y, toTypeName(y) FROM t AS o;
SELECT sumSimpleStateIf(toNullable(number), number > 0) AS x, toTypeName(x), hex(sumSimpleStateIfState(toNullable(number), number > 0)) FROM numbers(3);
SELECT nullIf(m, 0) FROM t; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT r.m FROM (SELECT 2 AS k) AS l LEFT JOIN t AS r ON l.k = r.k SETTINGS join_use_nulls = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
