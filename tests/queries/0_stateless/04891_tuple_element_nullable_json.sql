-- Nullable(JSON) in tupleElement, i.e. nested property access on a nullable JSON column.

-- Untyped path stays Dynamic and carries the outer NULLs itself.
select '{"a":42}'::Nullable(JSON).a as x, toTypeName(x);
select null::Nullable(JSON).a as x, toTypeName(x);
select (number % 2 ? null : '{"a":42}')::Nullable(JSON).a as x from numbers(4) order by number;

-- Missing path.
select '{"a":1}'::Nullable(JSON).zzz as x, toTypeName(x);

-- A typed path is promoted to Nullable(T).
select cast('{"a":42}', 'Nullable(JSON(a UInt32))').a as x, toTypeName(x);
select cast(null, 'Nullable(JSON(a UInt32))').a as x, toTypeName(x);

-- Array(Nullable(JSON)).
select cast(['{"a":1}', null], 'Array(Nullable(JSON))').a as x, toTypeName(x);

-- Chained access through a nullable sub-object, from issue #111234. Ordered by a companion Int rather
-- than by the result, which is Dynamic and so rejected in ORDER BY keys by default. `enable_analyzer` is
-- pinned because the old analyzer resolves the `^` sub-object subcolumn of a subquery column by looking up
-- a column of that literal name in the block, which fails with `NOT_FOUND_COLUMN_IN_BLOCK`.
select Doc.^Value::Nullable(JSON).Id as x
from
(
    select 1 as n, '{"Value": null}'::JSON as Doc
    union all
    select 2 as n, '{"Value": {"Id": "dorki"}}'::JSON as Doc
)
order by n
settings enable_analyzer = 1;

-- The query exactly as filed: Doc.Value is the leaf Dynamic of a sub-object path, so it is NULL on both
-- rows. It must return NULL rather than throw. `enable_analyzer` is pinned for the same reason as above:
-- the old analyzer looks up a column literally named `Doc.Value` in the block instead of extracting the
-- path from the JSON column, so any subcolumn of a subquery column fails there regardless of this change.
select Doc.Value::Nullable(JSON).Id as x
from
(
    select 1 as n, '{"Value": null}'::JSON as Doc
    union all
    select 2 as n, '{"Value": {"Id": "dorki"}}'::JSON as Doc
)
order by n
settings enable_analyzer = 1;

-- A typed `Array(...)` / `Map(...)` path can neither be wrapped in `Nullable` nor carry NULL itself, so
-- outer-NULL rows must read as the path default ([] / {}) -- matching a stored subcolumn and the
-- `Nullable(Tuple(...))` case -- rather than the payload that stays in the hidden nested `ColumnObject`
-- under the null map. The `if(...)` below nulls the outer JSON while leaving `[3,4]` / `{'k':'v'}` in that
-- hidden column, and `materialize(...)` forces the non-optimized function path.
select number, tupleElement(materialize(n), 'arr') as x, toTypeName(x)
from
(
    select number, if(number = 1, null, cast('{"arr":[3,4]}', 'JSON(arr Array(UInt32))'))::Nullable(JSON(arr Array(UInt32))) as n
    from numbers(2)
)
order by number;

select number, tupleElement(materialize(n), 'm') as x, toTypeName(x)
from
(
    select number, if(number = 1, null, cast('{"m":{"k":"v"}}', 'JSON(m Map(String, String))'))::Nullable(JSON(m Map(String, String))) as n
    from numbers(2)
)
order by number;

-- Nullable(QBit) is still rejected.
select cast(materialize([1., 2., 3., 4.]), 'Nullable(QBit(Float32, 4))').1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
