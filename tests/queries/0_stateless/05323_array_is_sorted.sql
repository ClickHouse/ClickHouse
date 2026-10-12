-- { echoOn }

-- Basic behavior.
SELECT arrayIsSorted([]), arrayIsSorted([1]), arrayIsSorted([1, 2, 2, 3]), arrayIsSorted([1, 3, 2]);

-- Strings and compound values.
SELECT arrayIsSorted(['a', 'b', 'b']), arrayIsSorted(['a', 'c', 'b']), arrayIsSorted([(1, 'a'), (1, 'b'), (2, 'a')]), arrayIsSorted([(1, 'b'), (1, 'a')]);

-- Floating-point and nullable values.
SELECT arrayIsSorted([-inf, 0.0, inf, nan]), arrayIsSorted([1.0, nan]), arrayIsSorted([nan, 1.0]), arrayIsSorted([1, 2, NULL]), arrayIsSorted([1, NULL, 2]);

-- Keep comparisons inside each array row.
SELECT arrayIsSorted(a)
FROM values(
    'a Array(Int32)',
    ([100, 200]),
    ([-100, -50]),
    ([5, 3]),
    ([])
);

-- LowCardinality ordering must follow values rather than dictionary indexes.
SELECT arrayIsSorted(a)
FROM values(
    'a Array(LowCardinality(String))',
    (['z']),
    (['a', 'z']),
    (['z', 'a'])
);

SELECT arrayIsSorted(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Match arraySort's special-value order: regular values, NaN, then NULL.
-- { echoOff }
SELECT throwIf(arrayIsSorted([-inf, 0.0, inf, nan, NULL]) != 1) FORMAT Null;
SELECT throwIf(arrayIsSorted([-inf, 0.0, inf, NULL, nan]) != 0) FORMAT Null;
