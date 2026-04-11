-- { echo }

SET allow_experimental_nullable_tuple_type = 1;
SET optimize_functions_to_subcolumns = 0;

-- MergeTree Wide format
CREATE TABLE t_wide (tup Nullable(Tuple(u UInt64, s Nullable(String)))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_wide SELECT if((number % 2) = 0, (number, toString(number)), NULL) FROM numbers(4);
SELECT tup, tup.s, tup.u, isNull(tup), isNull(tup.s), isNull(tup.u) FROM t_wide ORDER BY tup.u;

-- MergeTree Compact format
CREATE TABLE t_compact (tup Nullable(Tuple(u UInt64, s Nullable(String)))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000;
INSERT INTO t_compact SELECT if((number % 2) = 0, (number, toString(number)), NULL) FROM numbers(4);
SELECT tup, tup.s, tup.u, isNull(tup), isNull(tup.s), isNull(tup.u) FROM t_compact ORDER BY tup.u;

-- Memory engine
CREATE TABLE t_mem (tup Nullable(Tuple(u UInt64, s Nullable(String)))) ENGINE = Memory;
INSERT INTO t_mem SELECT if((number % 2) = 0, (number, toString(number)), NULL) FROM numbers(4);
SELECT tup, tup.s, tup.u, isNull(tup), isNull(tup.s), isNull(tup.u) FROM t_mem ORDER BY tup.u;

-- count should skip rows where outer tuple is NULL
SELECT count(tup.s), count(tup.u) FROM t_wide;
SELECT count(tup.s), count(tup.u) FROM t_compact;
SELECT count(tup.s), count(tup.u) FROM t_mem;

-- Reading only the nullable subcolumn (no other columns from same tuple)
SELECT tup.s FROM t_wide ORDER BY tup.s;

-- INSERT VALUES should also work correctly
CREATE TABLE t_values (tup Nullable(Tuple(s Nullable(String)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_values VALUES (('hello')), (NULL), ((NULL));
SELECT tup, tup.s, isNull(tup), isNull(tup.s) FROM t_values ORDER BY tup.s;
