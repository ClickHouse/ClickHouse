SELECT arrayEnumerateUniqRanked(arrayEnumerateUniqRanked([[1, 2, 3], [2, 2, 1], [3]]), materialize(1 AS x) OR toLowCardinality(-9223372036854775808)); -- { serverError BAD_ARGUMENTS }
SELECT arrayEnumerateUniqRanked([[1, 2, 3], [2, 2, 1], [3]], number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
