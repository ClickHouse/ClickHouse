-- { echo }
-- The `tupleElement` function must agree with the subcolumn path (`t.a`) on both the result type and the
-- values, including for `LowCardinality` elements. Previously `tupleElement` inherited the default
-- LowCardinality implementation, which stripped a nested `LowCardinality(...)` element via
-- `recursiveRemoveLowCardinality` before extraction, so `tupleElement(t, 'a')` returned `String` /
-- `Nullable(String)` while `t.a` returned `LowCardinality(String)` / `LowCardinality(Nullable(String))`.
-- `materialize(...)` forces the non-optimized function path (the subcolumn optimization cannot fire).

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_04344;
CREATE TABLE t_04344
(
    t Tuple(a LowCardinality(String), b UInt32),
    n Nullable(Tuple(a LowCardinality(String), lcn LowCardinality(Nullable(String)), s String, m Map(String, String)))
)
ENGINE = Memory;

INSERT INTO t_04344 VALUES (('x', 1), ('p', 'q', 's', map('k', 'v'))), (('y', 2), NULL);

-- Plain Tuple: element stays LowCardinality(String) on both the subcolumn and the function paths.
SELECT toTypeName(t.a), toTypeName(tupleElement(t, 'a')), toTypeName(tupleElement(materialize(t), 'a')) FROM t_04344 LIMIT 1;
SELECT t.a, tupleElement(t, 'a'), tupleElement(materialize(t), 'a') FROM t_04344 ORDER BY t.b;

-- Nullable(Tuple): a non-nullable LowCardinality(String) element is promoted to
-- LowCardinality(Nullable(String)) and reads NULL for outer-NULL rows on every path.
SELECT toTypeName(n.a), toTypeName(tupleElement(n, 'a')), toTypeName(tupleElement(materialize(n), 'a')) FROM t_04344 LIMIT 1;
SELECT n.a, tupleElement(n, 'a'), tupleElement(materialize(n), 'a') FROM t_04344 ORDER BY t.b;

-- LowCardinality(Nullable(String)) element already carries NULL: type and values stay consistent.
SELECT toTypeName(n.lcn), toTypeName(tupleElement(n, 'lcn')), toTypeName(tupleElement(materialize(n), 'lcn')) FROM t_04344 LIMIT 1;
SELECT n.lcn, tupleElement(n, 'lcn'), tupleElement(materialize(n), 'lcn') FROM t_04344 ORDER BY t.b;

-- Plain String element inside Nullable(Tuple) wraps into Nullable(String) on every path.
SELECT toTypeName(n.s), toTypeName(tupleElement(n, 's')), toTypeName(tupleElement(materialize(n), 's')) FROM t_04344 LIMIT 1;
SELECT n.s, tupleElement(n, 's'), tupleElement(materialize(n), 's') FROM t_04344 ORDER BY t.b;

-- Map element cannot be wrapped into Nullable, so it stays Map(String, String) and outer-NULL rows read as
-- the default empty map on every path (unchanged behavior).
SELECT toTypeName(n.m), toTypeName(tupleElement(n, 'm')), toTypeName(tupleElement(materialize(n), 'm')) FROM t_04344 LIMIT 1;
SELECT n.m, tupleElement(n, 'm'), tupleElement(materialize(n), 'm') FROM t_04344 ORDER BY t.b;

DROP TABLE t_04344;

-- A `Map` / `Array` element of a `Nullable(Tuple(...))` can neither be wrapped in `Nullable` nor carry
-- NULL itself, so outer-NULL rows must read as the element default ({} / []) -- matching a stored subcolumn
-- and `NestedUtils::unwrapNullableTuple`. The construction below nulls the outer tuple with `if(...)` while
-- leaving a non-default payload ({'k2':'v2'} / [3,4]) in the hidden nested column, and `materialize(...)`
-- forces the non-optimized function path. The outer-NULL row must read the default, not that hidden payload.
SELECT n, tupleElement(materialize(n), 'm') AS via_function
FROM (
    SELECT if(t.1 = 'b', NULL, t)::Nullable(Tuple(a String, m Map(String, String))) AS n
    FROM (SELECT arrayJoin([tuple('a', map('k1', 'v1'))::Tuple(a String, m Map(String, String)),
                            tuple('b', map('k2', 'v2'))::Tuple(a String, m Map(String, String))]) AS t)
)
SETTINGS allow_experimental_nullable_tuple_type = 1;

SELECT n, tupleElement(materialize(n), 'arr') AS via_function
FROM (
    SELECT if(t.1 = 'b', NULL, t)::Nullable(Tuple(a String, arr Array(UInt32))) AS n
    FROM (SELECT arrayJoin([tuple('a', [1, 2])::Tuple(a String, arr Array(UInt32)),
                            tuple('b', [3, 4])::Tuple(a String, arr Array(UInt32))]) AS t)
)
SETTINGS allow_experimental_nullable_tuple_type = 1;
