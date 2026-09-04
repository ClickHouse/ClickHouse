-- Two streams of a single column that render to the same file name are written into one file, so the
-- part cannot be read back. Such a column must be rejected at CREATE/ALTER, like a collision between
-- two different columns already is.

DROP TABLE IF EXISTS t_collision;

-- A JSON typed path named like an automatic substream of an enclosing Array/Nullable.
CREATE TABLE t_collision (c Array(JSON(`size0` Int64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Array(Array(JSON(`size1` Int64)))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Nullable(JSON(`null` Int64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Array(Nullable(JSON(`size0` Int64)))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Array(Nullable(JSON(`null` Int64)))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A JSON typed path named like one of JSON's own internal streams.
CREATE TABLE t_collision (c JSON(`object_structure` Int64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- A name with a dot renders to the same file name as the same name split across two components,
-- because the dot separator and an escaped dot inside a component are indistinguishable.
CREATE TABLE t_collision (c JSON(`a` Tuple(`b` Int64), `a.b` Int64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c JSON(`a` Map(String, Int64), `a.keys` Int64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Tuple(`a` Tuple(`b` UInt64), `a.b` UInt64)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_collision (c Tuple(`a.keys` Array(String), `a` Map(String, UInt64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The same collision one level deeper, and in a column that is not the only one in the table.
CREATE TABLE t_collision (i UInt64, c Tuple(`t` Tuple(`a` Tuple(`b` UInt64), `a.b` UInt64))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- ALTER must not be able to turn a healthy column into a colliding one.
CREATE TABLE t_collision (c Array(JSON(`x` Int64))) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_collision MODIFY COLUMN c Array(JSON(`size0` Int64)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_collision ADD COLUMN d Tuple(`a` Tuple(`b` UInt64), `a.b` UInt64); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_collision;

-- A collision between two columns is still detected.
CREATE TABLE t_collision (`a` Tuple(`b` UInt64), `a.b` UInt64) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Streams of one column that only collide as subcolumn names, not as file names, stay allowed: the
-- whole column is written and read back correctly, so such tables must keep working.
CREATE TABLE t_collision (c Array(Tuple(`size0` UInt64))) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
CREATE TABLE t_collision (c Tuple(`a` String, `a.size` UInt64)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
CREATE TABLE t_collision (c Tuple(`a` Array(UInt64), `a.size0` UInt64)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
CREATE TABLE t_collision (c Tuple(`a` Nullable(UInt64), `a.null` UInt8)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
CREATE TABLE t_collision (c JSON(`a` Array(Int64), `a.size0` Int64)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
CREATE TABLE t_collision (c JSON(`a` JSON(`b` Int64), `a.b` Int64)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;
CREATE TABLE t_collision (c Nullable(JSON)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_collision;

-- Nested types that are written and read every day must not be reported as colliding.
SET enable_nullable_tuple_type = 1;
CREATE TABLE t_collision
(
    a Array(Array(Array(Nullable(String)))),
    b Map(String, Array(Nullable(UInt64))),
    c Tuple(x Nullable(String), y Array(Tuple(z LowCardinality(Nullable(String))))),
    d Nullable(Tuple(p Int64, q Array(String))),
    e Variant(UInt64, Array(String), Map(String, Nullable(Int64))),
    f Dynamic,
    g JSON(`p.q` Int64, r Array(Tuple(s Nullable(String)))),
    h Array(JSON),
    i LowCardinality(Nullable(String)),
    j Nested(k UInt64, l Array(Nullable(String)))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_collision SELECT [[['a', NULL]]], map('k', [1, NULL]), ('s', [tuple('t')]), (1, ['u']),
    ['v']::Variant(UInt64, Array(String), Map(String, Nullable(Int64))), 42::Dynamic,
    '{"p.q":1,"r":[{"s":"w"}]}', ['{"x":1}'], 'y', [1], [['z']];
SELECT count() FROM t_collision;
DROP TABLE t_collision;
