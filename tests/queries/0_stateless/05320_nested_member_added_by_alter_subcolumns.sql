-- Subcolumns of Nested members added by ALTER ADD COLUMN, read from data written before the ALTER (k < 10), equal those of stored defaults (k > 10).

SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nested_add_wide;
DROP TABLE IF EXISTS t_nested_add_compact;
DROP TABLE IF EXISTS t_nested_add_memory;

CREATE TABLE t_nested_add_wide (k UInt64, n Nested(a UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, share_nested_offsets = 1;
CREATE TABLE t_nested_add_compact (k UInt64, n Nested(a UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, share_nested_offsets = 1;
CREATE TABLE t_nested_add_memory (k UInt64, n Nested(a UInt64)) ENGINE = Memory;

SYSTEM STOP MERGES t_nested_add_wide;
SYSTEM STOP MERGES t_nested_add_compact;

INSERT INTO t_nested_add_wide VALUES (1, [10, 20]), (2, []), (3, [30]);
INSERT INTO t_nested_add_compact VALUES (1, [10, 20]), (2, []), (3, [30]);
INSERT INTO t_nested_add_memory VALUES (1, [10, 20]), (2, []), (3, [30]);

ALTER TABLE t_nested_add_wide
    ADD COLUMN `n.c` Array(Array(UInt64)),
    ADD COLUMN `n.e` Array(Array(Array(UInt8))),
    ADD COLUMN `n.m` Array(Map(String, UInt64)),
    ADD COLUMN `n.t` Array(Tuple(p Array(UInt8), q UInt8)),
    ADD COLUMN `n.nt` Array(Nullable(Tuple(p Array(UInt8), q UInt8))),
    ADD COLUMN `n.v` Array(Variant(Array(UInt8), String)),
    ADD COLUMN `n.j` Array(JSON(x Array(UInt8))),
    ADD COLUMN `n.d` Array(Dynamic),
    ADD COLUMN `n.lc` Array(Array(LowCardinality(String)));
ALTER TABLE t_nested_add_compact
    ADD COLUMN `n.c` Array(Array(UInt64)),
    ADD COLUMN `n.e` Array(Array(Array(UInt8))),
    ADD COLUMN `n.m` Array(Map(String, UInt64)),
    ADD COLUMN `n.t` Array(Tuple(p Array(UInt8), q UInt8)),
    ADD COLUMN `n.nt` Array(Nullable(Tuple(p Array(UInt8), q UInt8))),
    ADD COLUMN `n.v` Array(Variant(Array(UInt8), String)),
    ADD COLUMN `n.j` Array(JSON(x Array(UInt8))),
    ADD COLUMN `n.d` Array(Dynamic),
    ADD COLUMN `n.lc` Array(Array(LowCardinality(String)));
ALTER TABLE t_nested_add_memory
    ADD COLUMN `n.c` Array(Array(UInt64)),
    ADD COLUMN `n.e` Array(Array(Array(UInt8))),
    ADD COLUMN `n.m` Array(Map(String, UInt64)),
    ADD COLUMN `n.t` Array(Tuple(p Array(UInt8), q UInt8)),
    ADD COLUMN `n.nt` Array(Nullable(Tuple(p Array(UInt8), q UInt8))),
    ADD COLUMN `n.v` Array(Variant(Array(UInt8), String)),
    ADD COLUMN `n.j` Array(JSON(x Array(UInt8))),
    ADD COLUMN `n.d` Array(Dynamic),
    ADD COLUMN `n.lc` Array(Array(LowCardinality(String)));

INSERT INTO t_nested_add_wide (k, n.a, n.c, n.e, n.m, n.t, n.nt, n.v, n.j, n.d, n.lc) VALUES
    (11, [10, 20], [[], []], [[], []], [{}, {}], [([], 0), ([], 0)], [NULL, NULL], [NULL, NULL], ['{}', '{}'], [NULL, NULL], [[], []]),
    (12, [], [], [], [], [], [], [], [], [], []),
    (13, [30], [[]], [[]], [{}], [([], 0)], [NULL], [NULL], ['{}'], [NULL], [[]]);
INSERT INTO t_nested_add_compact (k, n.a, n.c, n.e, n.m, n.t, n.nt, n.v, n.j, n.d, n.lc) VALUES
    (11, [10, 20], [[], []], [[], []], [{}, {}], [([], 0), ([], 0)], [NULL, NULL], [NULL, NULL], ['{}', '{}'], [NULL, NULL], [[], []]),
    (12, [], [], [], [], [], [], [], [], [], []),
    (13, [30], [[]], [[]], [{}], [([], 0)], [NULL], [NULL], ['{}'], [NULL], [[]]);
INSERT INTO t_nested_add_memory (k, n.a, n.c, n.e, n.m, n.t, n.nt, n.v, n.j, n.d, n.lc) VALUES
    (11, [10, 20], [[], []], [[], []], [{}, {}], [([], 0), ([], 0)], [NULL, NULL], [NULL, NULL], ['{}', '{}'], [NULL, NULL], [[], []]),
    (12, [], [], [], [], [], [], [], [], [], []),
    (13, [30], [[]], [[]], [{}], [([], 0)], [NULL], [NULL], ['{}'], [NULL], [[]]);

-- The part with k < 10 must not store the added members.
SELECT has_c, count() FROM
(
    SELECT name, countIf(column = 'n.c') AS has_c FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_add_wide' AND active GROUP BY name
)
GROUP BY has_c ORDER BY has_c;
SELECT has_c, count() FROM
(
    SELECT name, countIf(column = 'n.c') AS has_c FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_nested_add_compact' AND active GROUP BY name
)
GROUP BY has_c ORDER BY has_c;

SELECT k, n.c.size1 FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.e.size1), materialize(n.e.size2) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.m.size1) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.t.p.size1) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.nt.p.size1), materialize(n.nt.p) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.v.`Array(UInt8)`.size1) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.d.`Array(UInt8)`.size1), materialize(n.lc.size1) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.t.q), materialize(n.m.keys), materialize(n.nt.null) FROM t_nested_add_wide ORDER BY k;
SELECT k, materialize(n.j.x), materialize(n.j.x.size0), materialize(n.j.x.size1), arraySum(n.j.x.size1) FROM t_nested_add_wide ORDER BY k;
SELECT k, n.a, materialize(n.c.size1) FROM t_nested_add_wide ORDER BY k;

SELECT k, n.c.size1 FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.e.size1), materialize(n.e.size2) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.m.size1) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.t.p.size1) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.nt.p.size1), materialize(n.nt.p) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.v.`Array(UInt8)`.size1) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.d.`Array(UInt8)`.size1), materialize(n.lc.size1) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.t.q), materialize(n.m.keys), materialize(n.nt.null) FROM t_nested_add_compact ORDER BY k;
SELECT k, materialize(n.j.x), materialize(n.j.x.size0), materialize(n.j.x.size1), arraySum(n.j.x.size1) FROM t_nested_add_compact ORDER BY k;
SELECT k, n.a, materialize(n.c.size1) FROM t_nested_add_compact ORDER BY k;

SELECT k, n.a, n.c.size1 FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.e.size1), materialize(n.e.size2) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.m.size1) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.t.p.size1) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.nt.p.size1), materialize(n.nt.p) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.v.`Array(UInt8)`.size1) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.d.`Array(UInt8)`.size1), materialize(n.lc.size1) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.t.q), materialize(n.m.keys), materialize(n.nt.null) FROM t_nested_add_memory ORDER BY k;
SELECT k, n.a, materialize(n.j.x), materialize(n.j.x.size0), materialize(n.j.x.size1), arraySum(n.j.x.size1) FROM t_nested_add_memory ORDER BY k;

DROP TABLE t_nested_add_wide;
DROP TABLE t_nested_add_compact;
DROP TABLE t_nested_add_memory;

-- A FINAL merge combines a part written before the ALTER with one that stores the added member.
DROP TABLE IF EXISTS t_nested_add_final;
CREATE TABLE t_nested_add_final (id UInt64, v UInt64, `p.x` Array(Int64))
ENGINE = ReplacingMergeTree(v) ORDER BY id;
SYSTEM STOP MERGES t_nested_add_final;
INSERT INTO t_nested_add_final VALUES (1, 1, [1, 2]), (2, 1, [3]);
ALTER TABLE t_nested_add_final ADD COLUMN `p.j` Array(JSON);
INSERT INTO t_nested_add_final VALUES (1, 2, [7, 8, 9], ['{"r":["a","b"]}', '{"r":["c",1]}', '{"r":5}']), (2, 2, [10, 11], ['{"r":[1,2]}', '{"r":[2.5,"d",null]}']);
SELECT id, v, p.x, p.j.r.:`Array(Dynamic)`, p.j.r.:`Array(Nullable(String))` FROM t_nested_add_final FINAL ORDER BY id;
DROP TABLE t_nested_add_final;
