SET allow_suspicious_types_in_order_by = 1;

-- Indexing an empty array with a Dynamic argument returns NULL for every row.

-- One stored type, no NULLs.
SELECT arrayElement([], d) FROM (SELECT CAST(1, 'Dynamic') AS d);

-- One stored type plus NULLs.
SELECT arrayElement([], d) FROM (SELECT if(number % 2, CAST(number, 'Dynamic'), CAST(NULL, 'Dynamic')) AS d FROM numbers(4));

-- Several stored types.
SELECT arrayElement([], d) FROM (SELECT if(number % 2, CAST(toInt64(number), 'Dynamic'), CAST(toUInt8(number), 'Dynamic')) AS d FROM numbers(4));

-- Several stored types plus NULLs.
SELECT arrayElement([], d) FROM (SELECT multiIf(number % 3 = 0, CAST(toInt64(number), 'Dynamic'), number % 3 = 1, CAST(toUInt8(number), 'Dynamic'), CAST(NULL, 'Dynamic')) AS d FROM numbers(6));

-- The empty array may also be a non-constant column, or the result of another function.
SELECT arrayElement(materialize([]), d) FROM (SELECT if(number % 2, CAST(number, 'Dynamic'), CAST(NULL, 'Dynamic')) AS d FROM numbers(4));
SELECT arrayElement(arrayPopFront([]), d) FROM (SELECT CAST(1, 'Dynamic') AS d);

-- The empty array may be the Dynamic value itself.
SELECT arrayElement(d, 1) FROM (SELECT CAST([], 'Dynamic') AS d);
SELECT arrayElement(d, 1) FROM (SELECT if(number % 2, CAST([], 'Dynamic'), CAST(NULL, 'Dynamic')) AS d FROM numbers(4));

-- A non-empty array is still indexed normally.
SELECT arrayElement(d, 1) FROM (SELECT CAST([7, 8], 'Dynamic') AS d);
SELECT arrayElement([10, 20], d) FROM (SELECT CAST(2, 'Dynamic') AS d);

-- An argument that cannot index an array is still rejected.
SELECT arrayElement([], d) FROM (SELECT CAST('s', 'Dynamic') AS d); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
