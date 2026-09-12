-- A member of a `Nullable(Tuple(...))` whose own type cannot be inside `Nullable` (a `Variant`) is
-- exposed bare, while a member that can be is wrapped by the enclosing null map. Every query below
-- must give the same answer with the optimization on and off.

SET enable_analyzer = 1;
SET enable_nullable_tuple_type = 1;
SET enable_variant_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple_element;
DROP TABLE IF EXISTS t_plain_tuple_element;

CREATE TABLE t_nullable_tuple_element
(
    key UInt64,
    t Nullable(Tuple(inner Tuple(x UInt64, a Nullable(UInt64)), v Variant(Int64, String)))
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_nullable_tuple_element VALUES (1, ((10, 1), 5::Int64)), (2, NULL), (3, ((30, NULL), 'str'));

-- `t.inner` is `Nullable(Tuple(...))`, so `t.inner.x` is `Nullable(UInt64)` both in storage and as
-- the result of `tupleElement`, and the row whose whole tuple is NULL reads as NULL.

SELECT 'nested tuple element';
SELECT key, tupleElement(t.inner, 'x'), toTypeName(tupleElement(t.inner, 'x')) FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key, tupleElement(t.inner, 'x'), toTypeName(tupleElement(t.inner, 'x')) FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'nested tuple element, ordinal';
SELECT key, tupleElement(t.inner, 1) FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key, tupleElement(t.inner, 1) FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;

-- An aggregate is instantiated from the declared type, so a `Nullable` column reaching it aborts.

SELECT 'nested tuple element, aggregated';
SELECT sum(tupleElement(t.inner, 'x')) FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(tupleElement(t.inner, 'x')) FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 1;

-- `a` is already `Nullable`, so the enclosing null map does not change its type.

SELECT 'nested tuple element, already nullable';
SELECT key, tupleElement(t.inner, 'a') FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key, tupleElement(t.inner, 'a') FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(tupleElement(t.inner, 'a')) FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(tupleElement(t.inner, 'a')) FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 1;

-- A `Variant` element subcolumn is `Nullable(T)` in storage and `variantElement` is declared
-- `Nullable(T)`, so the enclosing null map leaves them in agreement.

SELECT 'variant element';
SELECT key, variantElement(t.v, 'Int64'), toTypeName(variantElement(t.v, 'Int64')) FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key, variantElement(t.v, 'Int64'), toTypeName(variantElement(t.v, 'Int64')) FROM t_nullable_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE t_plain_tuple_element
(
    key UInt64,
    t Tuple(inner Tuple(x UInt64), v Variant(Int64, String))
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_plain_tuple_element VALUES (1, tuple(tuple(10), 5::Int64)), (2, tuple(tuple(20), 'str'));

SELECT 'plain tuple element';
SELECT key, tupleElement(t.inner, 'x'), variantElement(t.v, 'Int64') FROM t_plain_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 0;
SELECT key, tupleElement(t.inner, 'x'), variantElement(t.v, 'Int64') FROM t_plain_tuple_element ORDER BY key SETTINGS optimize_functions_to_subcolumns = 1;

-- The answers above are equal whether or not a rewrite happened, so assert which rewrites fire.
-- `t.inner` is `Nullable(Tuple(...))`, and `FunctionToSubcolumnsPass` keys its transformers on
-- `Tuple`, so no `tupleElement` over it is rewritten. `t.v` is a `Variant`, which cannot be inside
-- `Nullable`, so it stays bare and its rewrite fires, as do both rewrites on the plain tuple.

SELECT 'rewrite fired';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t.inner, 'x') FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%t.inner.x%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t.inner, 1) FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%t.inner.x%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t.inner, 'a') FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%t.inner.a%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT variantElement(t.v, 'Int64') FROM t_nullable_tuple_element SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%t.v.Int64%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT tupleElement(t.inner, 'x') FROM t_plain_tuple_element SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%t.inner.x%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT variantElement(t.v, 'Int64') FROM t_plain_tuple_element SETTINGS optimize_functions_to_subcolumns = 1) WHERE explain ILIKE '%t.v.Int64%';

DROP TABLE t_nullable_tuple_element;
DROP TABLE t_plain_tuple_element;
