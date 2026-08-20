-- Tags: no-fasttest

-- Pass some random but valid scalar sub-queries as arguments to the `mergeTreeParts` function.
SELECT *
FROM mergeTreeParts((
    SELECT *
    FROM numbers(111, 1)
) AS arg1, (
    SELECT
        rand(),
        CAST('2025-02-26', 'Date'),
        '\0',
        CAST('NULL', 'String')
) AS arg2, (
    SELECT *
    FROM numbers(1)
) AS arg3, (
    SELECT
        positiveModulo(1, 2),
        ''
)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM mergeTreeParts(structure('x UInt8')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- An unknown top-level argument.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/'),
    something_else(x = 1)); -- { serverError BAD_ARGUMENTS }

-- An unknown part type.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(InMemory(path = 'a/', marks_count = 1, ranges = [(0, 1)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

-- A required setting is missing.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/'),
    table_settings()); -- { serverError BAD_ARGUMENTS }
