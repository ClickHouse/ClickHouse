-- Regression test: LowCardinality inside Tuple in IN subquery caused LOGICAL_ERROR
-- because Set::setHeader only stripped top-level LowCardinality from set_elements_types
-- while convertToFullIfNeeded (recursive since #97493) stripped it from inner columns,
-- creating a column/type mismatch in KeyCondition::tryPrepareSetColumnsForIndex.
SET allow_suspicious_low_cardinality_types = 1;

-- Exact reproducer from AST fuzzer CI report
DROP TABLE IF EXISTS table2__fuzz_36;
DROP TABLE IF EXISTS table1__fuzz_47;
CREATE TABLE table2__fuzz_36 (id1 Nullable(Date), id2 LowCardinality(Int8)) ENGINE = Memory AS SELECT 1, 1;
CREATE TABLE table1__fuzz_47 (id1 Nullable(Int8), id2 Decimal(76, 58)) ENGINE = MergeTree ORDER BY id1 SETTINGS allow_nullable_key = 1 AS SELECT 1, 1;
SELECT * FROM table1__fuzz_47 WHERE (id1, id2) IN (SELECT tuple(id2, id1) FROM table2__fuzz_36); -- { serverError ILLEGAL_COLUMN }
DROP TABLE table2__fuzz_36;
DROP TABLE table1__fuzz_47;

-- LowCardinality(String) inside tuple from subquery
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a LowCardinality(String), b Int32) ENGINE = Memory AS SELECT 'hello', 1;
CREATE TABLE t1 (a String, b Int32) ENGINE = MergeTree ORDER BY a AS SELECT 'hello', 1;
SELECT * FROM t1 WHERE (a, b) IN (SELECT tuple(a, b) FROM t2);
DROP TABLE t1;
DROP TABLE t2;

-- Multiple LowCardinality columns inside tuple, composite key
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a LowCardinality(String), b LowCardinality(Int32)) ENGINE = Memory AS SELECT 'world', 42;
CREATE TABLE t1 (a String, b Int32) ENGINE = MergeTree ORDER BY (a, b) AS SELECT 'world', 42;
SELECT * FROM t1 WHERE (a, b) IN (SELECT tuple(a, b) FROM t2);
DROP TABLE t1;
DROP TABLE t2;

-- Simple LowCardinality column in IN subquery (non-tuple case)
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a LowCardinality(String)) ENGINE = Memory AS SELECT 'simple';
CREATE TABLE t1 (a String) ENGINE = MergeTree ORDER BY a AS SELECT 'simple';
SELECT * FROM t1 WHERE a IN (SELECT a FROM t2);
DROP TABLE t1;
DROP TABLE t2;

-- LowCardinality(Nullable) inside tuple
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a LowCardinality(Nullable(String)), b Int32) ENGINE = Memory AS SELECT 'nullable', 5;
CREATE TABLE t1 (a Nullable(String), b Int32) ENGINE = MergeTree ORDER BY b AS SELECT 'nullable', 5;
SELECT * FROM t1 WHERE (a, b) IN (SELECT tuple(a, b) FROM t2);
DROP TABLE t1;
DROP TABLE t2;

-- NOT IN with LowCardinality in tuple
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a LowCardinality(String), b Int32) ENGINE = Memory AS SELECT 'excluded', 99;
CREATE TABLE t1 (a String, b Int32) ENGINE = MergeTree ORDER BY a AS SELECT 'kept', 1;
SELECT * FROM t1 WHERE (a, b) NOT IN (SELECT tuple(a, b) FROM t2);
DROP TABLE t1;
DROP TABLE t2;

-- Non-tuple subquery with multiple LowCardinality columns
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a LowCardinality(String), b Int32) ENGINE = Memory AS SELECT 'multi', 10;
CREATE TABLE t1 (a String, b Int32) ENGINE = MergeTree ORDER BY a AS SELECT 'multi', 10;
SELECT * FROM t1 WHERE (a, b) IN (SELECT a, b FROM t2);
DROP TABLE t1;
DROP TABLE t2;
