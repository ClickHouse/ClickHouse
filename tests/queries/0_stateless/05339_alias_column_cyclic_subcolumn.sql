-- An ALIAS column must not read itself through a subcolumn, directly or through other ALIAS columns.

CREATE TABLE t1 (c0 Int32, c5 String ALIAS c5.size) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t1 (c0 Int32, c5 String ALIAS `c5.size`) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t1 (c0 Int32, a String ALIAS toString(b.size), b String ALIAS a) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t1 (c0 Int32, b String ALIAS a, a String ALIAS toString(b.size)) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t1 (c0 Int32, t Tuple(x UInt64) ALIAS tuple(t.x)) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t1 (a Int32, `a.b` Tuple(x Int32) ALIAS tuple(`a.b.x`)) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
CREATE TABLE t1 (src Array(Tuple(hash String)), x Tuple(foo Tuple(hash String)) ALIAS tuple(tuple(concat(`x.foo`.hash, arrayStringConcat(arrayMap(`x.foo` -> `x.foo`.hash, src)))))) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

CREATE TABLE dst (c0 Int32, c5 String) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv TO dst (c0 Int32, c5 String ALIAS `c5.size`) AS SELECT 1 AS c0, 'a' AS c5; -- { serverError CYCLIC_ALIASES }

DESCRIBE TABLE format(CSV, 'c0 Int32, c5 String ALIAS c5.size', '1'); -- { serverError CYCLIC_ALIASES }

CREATE TABLE t0 (c0 Int32) ENGINE = Memory;
ALTER TABLE t0 ADD COLUMN c5 String ALIAS c5.size; -- { serverError CYCLIC_ALIASES }

CREATE TABLE t3 (c0 Int32, a String ALIAS toString(b.size), b String) ENGINE = Memory;
ALTER TABLE t3 MODIFY COLUMN b String ALIAS a; -- { serverError CYCLIC_ALIASES }

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (c0 Int32, c5 String ALIAS `c5.size`) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (s String, a String ALIAS concat(toString(length(s)) AS b, b), b String ALIAS toString(a.size)) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t3 (i Int64, a String ALIAS toString((SELECT b.size)), b String ALIAS a) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t4 (i Int64, a String ALIAS toString((SELECT a.size FROM numbers(1)))) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t5 (i Int64, a String ALIAS toString((SELECT count() FROM numbers(3)))) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t5 (i) VALUES (1);
SELECT a FROM {CLICKHOUSE_DATABASE_1:Identifier}.t5;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- Not cycles.
CREATE TABLE t4 (s String, a UInt64 ALIAS s.size, b String ALIAS 'abc', c UInt64 ALIAS b.size) ENGINE = Memory;
INSERT INTO t4 (s) VALUES ('hello');
SELECT a, b, c FROM t4;

CREATE TABLE t4_copy ENGINE = Memory AS t4;
INSERT INTO t4_copy (s) VALUES ('hi');
SELECT a, b, c FROM t4_copy;

CREATE TABLE t5 (`n.x` UInt64, n String ALIAS toString(`n.x`)) ENGINE = Memory;
INSERT INTO t5 VALUES (7);
SELECT n FROM t5;

CREATE TABLE t6 (a Tuple(b Int32), `a.c` Int32 ALIAS a.b) ENGINE = Memory;
INSERT INTO t6 VALUES (tuple(8));
SELECT `a.c` FROM t6;

CREATE TABLE t7 (id Int32, a Tuple(x Int32) DEFAULT tuple(b.x), b Tuple(x Int32) DEFAULT tuple(a.x)) ENGINE = Memory;
INSERT INTO t7 (id, a) VALUES (1, tuple(9));
SELECT * FROM t7;

CREATE TABLE t8 (src Array(Tuple(hash String)), x Tuple(foo Tuple(hash String)) ALIAS tuple(arrayFirst(`x.foo` -> length(`x.foo`.hash) > 0, src))) ENGINE = Memory;
INSERT INTO t8 (src) VALUES ([tuple(''), tuple('ab')]);
SELECT x FROM t8;

CREATE TABLE t9 (`n.x` UInt64, n Tuple(x UInt64) ALIAS tuple(`n.x`)) ENGINE = Memory;
INSERT INTO t9 VALUES (7);
SELECT n FROM t9;
