-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

DROP TYPE IF EXISTS ValUser;
DROP TYPE IF EXISTS ValTuple;
DROP TYPE IF EXISTS ValList;
DROP TYPE IF EXISTS ValAgg;
DROP TYPE IF EXISTS ValAggParam;
DROP TYPE IF EXISTS ValSimpleAgg;
DROP TYPE IF EXISTS ValVersionedAgg;
DROP TYPE IF EXISTS ValDynamic;
DROP TYPE IF EXISTS ValJSON;
DROP TYPE IF EXISTS ValBroken;

-- The arguments of a type that are not types (aggregate function names, settings, skipped JSON paths)
-- are not checked as type references.
CREATE TYPE ValAgg AS AggregateFunction(sum, UInt64);
CREATE TYPE ValAggParam(T) AS AggregateFunction(quantiles(0.5, 0.9), T);
CREATE TYPE ValSimpleAgg AS SimpleAggregateFunction(sum, UInt64);
CREATE TYPE ValVersionedAgg AS AggregateFunction(1, sumMap, Array(UInt8), Array(UInt64));
CREATE TYPE ValDynamic AS Dynamic(max_types = 10);
CREATE TYPE ValJSON AS JSON(max_dynamic_paths = 10, a.b UInt32, SKIP a.c, SKIP REGEXP 'x.*');
SELECT toTypeName(sumState(1::UInt64)::ValAgg);
SELECT toTypeName(quantilesState(0.5, 0.9)(1::Float64)::ValAggParam(Float64));
SELECT toTypeName(1::UInt64::ValSimpleAgg);
SELECT toTypeName(NULL::ValDynamic);
SELECT name, base_type FROM system.user_defined_types WHERE name LIKE 'Val%' ORDER BY name;

-- The types inside such definitions are still checked.
CREATE TYPE ValBroken AS AggregateFunction(sum, NoSuchType); -- { serverError UNKNOWN_TYPE }
CREATE TYPE ValBroken AS SimpleAggregateFunction(sum, NoSuchType); -- { serverError UNKNOWN_TYPE }
CREATE TYPE ValBroken AS JSON(a.b NoSuchType); -- { serverError UNKNOWN_TYPE }

-- A replacement must stay valid for the actual arguments the dependent types use it with,
-- not only for their number.
CREATE TYPE ValList(T) AS Array(T);
CREATE TYPE ValUser AS ValList(UInt8);
CREATE TYPE ValTuple(T) AS Tuple(T, ValList(String));
CREATE TYPE OR REPLACE ValList(T) AS Map(T); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TYPE OR REPLACE ValList(T) AS FixedString(T); -- { serverError UNEXPECTED_AST_STRUCTURE }
SHOW TYPE ValList;
SELECT toTypeName(CAST([1], 'ValUser'));

-- The use inside a parameterized type is checked when it does not depend on the parameters.
DROP TYPE ValUser;
CREATE TYPE OR REPLACE ValList(T) AS FixedString(T); -- { serverError UNEXPECTED_AST_STRUCTURE }
-- A use that depends on the parameters can only be checked when the type is used.
DROP TYPE ValTuple;
CREATE TYPE ValTuple(T) AS Tuple(ValList(T));
CREATE TYPE OR REPLACE ValList(T) AS FixedString(T);
SELECT toTypeName(CAST(tuple('ab'), 'ValTuple(2)'));

-- A compatible replacement is accepted and seen through the dependent types.
CREATE TYPE OR REPLACE ValList(T) AS Array(Nullable(T));
SELECT toTypeName(CAST(tuple([1]), 'ValTuple(UInt8)'));

DROP TYPE ValTuple;
DROP TYPE ValList;
DROP TYPE ValAgg;
DROP TYPE ValAggParam;
DROP TYPE ValSimpleAgg;
DROP TYPE ValVersionedAgg;
DROP TYPE ValDynamic;
DROP TYPE ValJSON;
SELECT count() FROM system.user_defined_types WHERE name LIKE 'Val%';
