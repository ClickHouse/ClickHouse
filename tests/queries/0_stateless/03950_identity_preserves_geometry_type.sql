-- `identity` / `__scalarSubqueryResult` must preserve a custom-named `Variant` type (`Geometry`)
-- and not fall through to the default `Variant` adaptor, which reassembles a bare `Variant` and
-- drops the custom name. See issue #110680 (`flipCoordinates`) for the same bug class.

WITH readWKT('POLYGON((0 0,4 0,4 3,0 3,0 0))') AS g
SELECT toTypeName(identity(g));

-- The preserved Geometry type can be passed directly to functions requiring a geometry.
WITH readWKT('POLYGON((0 0,4 0,4 3,0 3,0 0))') AS g
SELECT areaCartesian(identity(g));

-- Values round-trip unchanged across different Geometry subtypes in one column.
SELECT toTypeName(identity(g)) AS t, wkt(identity(g)) AS w
FROM (SELECT arrayJoin([readWKT('POINT(1 2)'), readWKT('POLYGON((0 0,4 0,4 3,0 3,0 0))')]) AS g)
ORDER BY w;

-- Plain (non-Geometry) Variant passthrough is unaffected: value and type preserved.
SELECT toTypeName(identity(x)) AS t, toString(identity(x)) AS v
FROM (SELECT CAST(if(number = 0, 'a', toString(number)), 'Variant(UInt64, String)') AS x FROM numbers(2))
ORDER BY v;

-- Plain scalar passthrough is unaffected.
SELECT toTypeName(identity(42)), identity(42);
