-- Redundant grouping around a scalar.
SELECT CAST(((1)) AS UInt128);
SELECT CAST((((1))) AS UInt128);

-- Redundant grouping inside an array.
SELECT CAST([(1)] AS Array(UInt128));
SELECT CAST([((1)),((2))] AS Array(UInt128));

-- Redundant grouping around a collection.
SELECT CAST(([0.1]) AS Array(Decimal32(2)));
SELECT CAST((([1,2])) AS Array(UInt128));

-- Real tuple parentheses must be preserved.
SELECT CAST(((1,2)) AS Tuple(UInt128, UInt128));

-- Redundant grouping around nested tuples.
SELECT CAST(
    ((1),((1,2)))
    AS Tuple(UInt128, Tuple(UInt128, UInt128))
);

-- Empty tuple must remain a tuple.
SELECT CAST(() AS Tuple());
