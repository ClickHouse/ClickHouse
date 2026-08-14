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
-- than by the result, which is Dynamic and so rejected in ORDER BY keys by default.
select Doc.^Value::Nullable(JSON).Id as x
from
(
    select 1 as n, '{"Value": null}'::JSON as Doc
    union all
    select 2 as n, '{"Value": {"Id": "dorki"}}'::JSON as Doc
)
order by n;

-- The query exactly as filed: Doc.Value is the leaf Dynamic of a sub-object path, so it is NULL on both
-- rows. It must return NULL rather than throw.
select Doc.Value::Nullable(JSON).Id as x
from
(
    select 1 as n, '{"Value": null}'::JSON as Doc
    union all
    select 2 as n, '{"Value": {"Id": "dorki"}}'::JSON as Doc
)
order by n;

-- Nullable(QBit) is still rejected.
select cast(materialize([1., 2., 3., 4.]), 'Nullable(QBit(Float32, 4))').1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
