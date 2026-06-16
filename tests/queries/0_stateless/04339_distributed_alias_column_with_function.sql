-- Tags: shard
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/97899
-- An ALIAS column read from a distributed/remote table together with a non-storage
-- function column and ORDER BY (so the query runs to WithMergeableState) used to be
-- routed into the wrong column slot under the analyzer, throwing CANNOT_PARSE_* or
-- silently returning wrong results.

DROP TABLE IF EXISTS t_04339;
DROP TABLE IF EXISTS t_04339_num;

CREATE TABLE t_04339 (x String, y String, xx String ALIAS concat(x, 'aaa'), yy String ALIAS concat(y, 'bbb'))
ENGINE = MergeTree ORDER BY x AS SELECT 'x', 'y';

CREATE TABLE t_04339_num (a UInt32, b UInt32, ax UInt32 ALIAS a + 100, bx UInt32 ALIAS b + 200)
ENGINE = MergeTree ORDER BY a AS SELECT 1, 2;

SET enable_analyzer = 1;

-- A function column BEFORE a plain column and its alias used to throw CANNOT_PARSE_DATETIME.
-- materialize() keeps the function value out of the output so the reference stays stable.
SELECT materialize(toDateTime('2020-01-01 00:00:00')) AS f, x, xx
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- Same with a plain (folded) constant column, no materialize.
SELECT toDateTime('2020-01-01 00:00:00') AS f, x, xx
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- A folded UInt8 constant before a plain column, its alias and another plain column.
SELECT 7 AS f, x, xx, y
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- Silent wrong result: function and alias share the UInt32 type, so the misrouted CAST
-- used to succeed and corrupt the values (ax became the function value).
SELECT materialize(7)::UInt32 AS f, a, ax, b
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339_num) ORDER BY a;

-- Two function columns interleaved with two alias columns.
SELECT materialize(7)::UInt32 AS f1, x, xx, materialize(8)::UInt32 AS f2, y, yy
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- Alias columns in reverse declaration order.
SELECT materialize(7)::UInt32 AS f, x, y, yy, xx
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- Alias between two storage columns.
SELECT materialize(7)::UInt32 AS f, x, xx, y
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- A value-named constant function shares the UInt32 type with the alias column. Its action-node
-- name carries the folded value and is identical in both plans, so it is matched by name and never
-- paired against the alias by position. The alias value must stay correct (ax = a + 100 = 101).
SELECT 42::UInt32 AS f, a, ax
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339_num) ORDER BY a;

-- Controls that already worked: function last, and no ORDER BY.
SELECT x, xx, materialize(7)::UInt32 AS f
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

SELECT materialize(7)::UInt32 AS f, x, xx
FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x;

-- The original issue shapes: a non-deterministic function next to a plain column and its ALIAS.
-- These used to throw CANNOT_PARSE_*; FORMAT Null proves they run (output is non-deterministic).
SELECT now(), x, xx FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x FORMAT Null;
SELECT generateUUIDv4(), x, xx FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x FORMAT Null;
SELECT rand(), x, xx FROM remote('127.0.0.{1,2}', currentDatabase(), t_04339) ORDER BY x FORMAT Null;

DROP TABLE t_04339;
DROP TABLE t_04339_num;
