-- { echo }

SET allow_experimental_nullable_tuple_type = 1;

-- MergeTree Wide format
DROP TABLE IF EXISTS t_wide;
CREATE TABLE t_wide (tup Nullable(Tuple(u UInt64, s Nullable(String)))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_wide SELECT if((number % 2) = 0, (number, toString(number)), NULL) FROM numbers(4);
SELECT tup, tup.s, tup.u, isNull(tup), isNull(tup.s), isNull(tup.u) FROM t_wide ORDER BY tup.u;

-- MergeTree Compact format
DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact (tup Nullable(Tuple(u UInt64, s Nullable(String)))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000;
INSERT INTO t_compact SELECT if((number % 2) = 0, (number, toString(number)), NULL) FROM numbers(4);
SELECT tup, tup.s, tup.u, isNull(tup), isNull(tup.s), isNull(tup.u) FROM t_compact ORDER BY tup.u;

-- Memory engine
DROP TABLE IF EXISTS t_mem;
CREATE TABLE t_mem (tup Nullable(Tuple(u UInt64, s Nullable(String)))) ENGINE = Memory;
INSERT INTO t_mem SELECT if((number % 2) = 0, (number, toString(number)), NULL) FROM numbers(4);
SELECT tup, tup.s, tup.u, isNull(tup), isNull(tup.s), isNull(tup.u) FROM t_mem ORDER BY tup.u;

-- count should skip rows where outer tuple is NULL
SELECT count(tup.s), count(tup.u) FROM t_wide;
SELECT count(tup.s), count(tup.u) FROM t_compact;
DROP TABLE t_compact;
SELECT count(tup.s), count(tup.u) FROM t_mem;
DROP TABLE t_mem;

-- Reading only the nullable subcolumn (no other columns from same tuple)
SELECT tup.s FROM t_wide ORDER BY tup.s;
DROP TABLE t_wide;

-- INSERT VALUES should also work correctly
DROP TABLE IF EXISTS t_values;
CREATE TABLE t_values (tup Nullable(Tuple(s Nullable(String)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_values VALUES (('hello')), (NULL), ((NULL));
SELECT tup, tup.s, isNull(tup), isNull(tup.s) FROM t_values ORDER BY tup.s, isNull(tup);
DROP TABLE t_values;

-- 3-level nested Nullable(Tuple(...)) -- Wide
DROP TABLE IF EXISTS t3_wide;
CREATE TABLE t3_wide (id UInt8, x Nullable(Tuple(a UInt32, t2 Nullable(Tuple(b UInt32, t3 Nullable(Tuple(c UInt32, leaf Nullable(Int64)))))))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t3_wide VALUES (0, NULL), (1, (10, NULL)), (2, (10, (20, NULL))), (3, (10, (20, (30, NULL)))), (4, (10, (20, (30, 42))));
SELECT id, x.t2, x.t2.t3, x.t2.t3.leaf, isNull(x.t2), isNull(x.t2.t3), isNull(x.t2.t3.leaf) FROM t3_wide ORDER BY id;
SELECT count(x.t2), count(x.t2.t3), count(x.t2.t3.leaf) FROM t3_wide;
DROP TABLE t3_wide;

-- 3-level nested Nullable(Tuple(...)) -- Compact
DROP TABLE IF EXISTS t3_compact;
CREATE TABLE t3_compact (id UInt8, x Nullable(Tuple(a UInt32, t2 Nullable(Tuple(b UInt32, t3 Nullable(Tuple(c UInt32, leaf Nullable(Int64)))))))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000;
INSERT INTO t3_compact VALUES (0, NULL), (1, (10, NULL)), (2, (10, (20, NULL))), (3, (10, (20, (30, NULL)))), (4, (10, (20, (30, 42))));
SELECT id, x.t2, x.t2.t3, x.t2.t3.leaf, isNull(x.t2), isNull(x.t2.t3), isNull(x.t2.t3.leaf) FROM t3_compact ORDER BY id;
SELECT count(x.t2), count(x.t2.t3), count(x.t2.t3.leaf) FROM t3_compact;
DROP TABLE t3_compact;

-- 3-level nested Nullable(Tuple(...)) -- Memory
DROP TABLE IF EXISTS t3_mem;
CREATE TABLE t3_mem (id UInt8, x Nullable(Tuple(a UInt32, t2 Nullable(Tuple(b UInt32, t3 Nullable(Tuple(c UInt32, leaf Nullable(Int64)))))))) ENGINE = Memory;
INSERT INTO t3_mem VALUES (0, NULL), (1, (10, NULL)), (2, (10, (20, NULL))), (3, (10, (20, (30, NULL)))), (4, (10, (20, (30, 42))));
SELECT id, x.t2, x.t2.t3, x.t2.t3.leaf, isNull(x.t2), isNull(x.t2.t3), isNull(x.t2.t3.leaf) FROM t3_mem ORDER BY id;
SELECT count(x.t2), count(x.t2.t3), count(x.t2.t3.leaf) FROM t3_mem;
DROP TABLE t3_mem;

-- 4-level nested Nullable(Tuple(...)) -- Wide
DROP TABLE IF EXISTS t4_wide;
CREATE TABLE t4_wide (id UInt8, x Nullable(Tuple(t2 Nullable(Tuple(t3 Nullable(Tuple(t4 Nullable(Tuple(c UInt32, leaf Nullable(Int64)))))))))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t4_wide VALUES (0, NULL), (1, (NULL)), (2, ((NULL))), (3, (((NULL)))), (4, ((((5, NULL))))), (5, ((((5, 99)))));
SELECT id, x.t2, x.t2.t3, x.t2.t3.t4, x.t2.t3.t4.leaf, isNull(x.t2), isNull(x.t2.t3), isNull(x.t2.t3.t4), isNull(x.t2.t3.t4.leaf) FROM t4_wide ORDER BY id;
SELECT count(x.t2), count(x.t2.t3), count(x.t2.t3.t4), count(x.t2.t3.t4.leaf) FROM t4_wide;
DROP TABLE t4_wide;

-- 4-level nested Nullable(Tuple(...)) -- Compact
DROP TABLE IF EXISTS t4_compact;
CREATE TABLE t4_compact (id UInt8, x Nullable(Tuple(t2 Nullable(Tuple(t3 Nullable(Tuple(t4 Nullable(Tuple(c UInt32, leaf Nullable(Int64)))))))))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000;
INSERT INTO t4_compact VALUES (0, NULL), (1, (NULL)), (2, ((NULL))), (3, (((NULL)))), (4, ((((5, NULL))))), (5, ((((5, 99)))));
SELECT id, x.t2, x.t2.t3, x.t2.t3.t4, x.t2.t3.t4.leaf, isNull(x.t2), isNull(x.t2.t3), isNull(x.t2.t3.t4), isNull(x.t2.t3.t4.leaf) FROM t4_compact ORDER BY id;
SELECT count(x.t2), count(x.t2.t3), count(x.t2.t3.t4), count(x.t2.t3.t4.leaf) FROM t4_compact;
DROP TABLE t4_compact;

-- 4-level nested Nullable(Tuple(...)) -- Memory
DROP TABLE IF EXISTS t4_mem;
CREATE TABLE t4_mem (id UInt8, x Nullable(Tuple(t2 Nullable(Tuple(t3 Nullable(Tuple(t4 Nullable(Tuple(c UInt32, leaf Nullable(Int64)))))))))) ENGINE = Memory;
INSERT INTO t4_mem VALUES (0, NULL), (1, (NULL)), (2, ((NULL))), (3, (((NULL)))), (4, ((((5, NULL))))), (5, ((((5, 99)))));
SELECT id, x.t2, x.t2.t3, x.t2.t3.t4, x.t2.t3.t4.leaf, isNull(x.t2), isNull(x.t2.t3), isNull(x.t2.t3.t4), isNull(x.t2.t3.t4.leaf) FROM t4_mem ORDER BY id;
SELECT count(x.t2), count(x.t2.t3), count(x.t2.t3.t4), count(x.t2.t3.t4.leaf) FROM t4_mem;
DROP TABLE t4_mem;
