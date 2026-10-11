-- An element of the first array whose value does not fit the type of the result is not a member of the
-- intersection. Its value truncated to that type can coincide with a value that is really there, and the
-- result must not emit it in the place of the overflowed element.

-- `map_arg != 0`: the second array is shorter, so it builds the lookup map.
SELECT arrayIntersect(CAST([257, 2, 1] AS Array(UInt16)), CAST([1, 2] AS Array(UInt8)));

-- `map_arg == 0`: the first array is shorter, so it builds the lookup map.
SELECT arrayIntersect(CAST([257, 3, 1] AS Array(UInt16)), CAST([1, 2, 3, 4, 5] AS Array(UInt8)));

-- The overflowed element is the only one that could match.
SELECT arrayIntersect(CAST([257] AS Array(UInt16)), CAST([1, 2] AS Array(UInt8)));

-- Non-const arguments, several rows.
SELECT arrayIntersect(a, b) FROM values('a Array(UInt16), b Array(UInt8)', ([257, 2, 1], [1, 2]), ([513, 5], [1, 5]), ([256], [0]));

-- The same for a `Date` result: a `DateTime` that is not midnight does not fit `Date`.
SELECT arrayIntersect(CAST(['2025-01-01 12:00:00', '2025-01-03 00:00:00', '2025-01-02 00:00:00'] AS Array(DateTime('UTC'))), CAST(['2025-01-01', '2025-01-02', '2025-01-03'] AS Array(Date)));

-- Commutativity is not affected.
SELECT arrayIntersect(CAST([1, 2] AS Array(UInt8)), CAST([257, 2, 1] AS Array(UInt16)));
