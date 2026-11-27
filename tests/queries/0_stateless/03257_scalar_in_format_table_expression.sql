SELECT * FROM format(
        JSONEachRow,
$$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
$$
    );

-- Should be equivalent to the previous one
SELECT * FROM format(
        JSONEachRow,
        (
            SELECT $$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
$$
        )
    );

-- The scalar subquery is incorrect so it should throw the proper error
SELECT * FROM format(
        JSONEachRow,
        (
            SELECT $$
{"a": "Hello", "b": 111}
{"a": "World", "b": 123}
$$
            WHERE column_does_not_exists = 4
        )
    ); -- { serverError UNKNOWN_IDENTIFIER }

-- https://github.com/ClickHouse/ClickHouse/issues/70177

-- Resolution of the scalar subquery should work ok (already did, adding a test just for safety)
-- Disabled for the old analyzer since it incorrectly passes 's' to format, instead of resolving s and passing that
WITH (SELECT sum(number)::String as s FROM numbers(4)) as s
SELECT *, s
FROM format(TSVRaw, s)
SETTINGS enable_analyzer=1;

SELECT count()
FROM format(TSVRaw, (
    SELECT where_qualified__fuzz_19
    FROM numbers(10000)
)); -- { serverError UNKNOWN_IDENTIFIER }

SELECT count()
FROM format(TSVRaw, (
    SELECT where_qualified__fuzz_19
    FROM numbers(10000)
    UNION ALL
    SELECT where_qualified__fuzz_35
    FROM numbers(10000)
)); -- { serverError UNKNOWN_IDENTIFIER }

WITH (
    SELECT where_qualified__fuzz_19
    FROM numbers(10000)
) as s SELECT count()
FROM format(TSVRaw, s); -- { serverError UNKNOWN_IDENTIFIER }

-- https://github.com/ClickHouse/ClickHouse/issues/70675
SELECT count()
FROM format(TSVRaw, (
    SELECT CAST(arrayStringConcat(groupArray(format(TSVRaw, (
            SELECT CAST(arrayStringConcat(1 GLOBAL IN (
                    SELECT 1
                    WHERE 1 GLOBAL IN (
                        SELECT toUInt128(1)
                        GROUP BY
                            GROUPING SETS ((1))
                            WITH ROLLUP
                    )
                    GROUP BY 1
                        WITH CUBE
                ), groupArray('some long string')), 'LowCardinality(String)')
            FROM numbers(10000)
        )), toLowCardinality('some long string')) RESPECT NULLS, '\n'), 'LowCardinality(String)')
    FROM numbers(10000)
))
FORMAT TSVRaw; -- { serverError UNKNOWN_IDENTIFIER, ILLEGAL_TYPE_OF_ARGUMENT }

-- Same but for table function numbers
SELECT 1 FROM numbers((SELECT DEFAULT)); -- { serverError UNKNOWN_IDENTIFIER }
