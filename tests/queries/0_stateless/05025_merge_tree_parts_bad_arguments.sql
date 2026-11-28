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

-- The part path must stay inside the root of the disk: an absolute path would replace the root when the
-- two are joined, and `..` components would step out of it.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = '/var/lib/clickhouse/store/aaa/', marks_count = 1, ranges = [(0, 1)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = '../../store/aaa/', marks_count = 1, ranges = [(0, 1)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'store/../../aaa/', marks_count = 1, ranges = [(0, 1)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

-- `index_granularity` cannot be zero: it is the granule size of a part with non-adaptive marks.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 0, index_granularity = 0)); -- { serverError BAD_ARGUMENTS }

-- An unknown table setting.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760, nonexistent_setting = 1)); -- { serverError BAD_ARGUMENTS }

-- The mark ranges drive the read scheduler directly, and `MarkRange` checks `begin <= end` only with an
-- assertion that release builds compile out, so every invalid shape must be rejected during parsing:
-- negative bounds, `begin >= end`, ends beyond `marks_count`, and unsorted or overlapping ranges.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'a/', marks_count = 1, ranges = [(-9223372036854775808, 255)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'a/', marks_count = 2, ranges = [(1, 0)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'a/', marks_count = 2, ranges = [(1, 1)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'a/', marks_count = 2, ranges = [(0, 3)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'a/', marks_count = 4, ranges = [(0, 2), (1, 3)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(Wide(path = 'a/', marks_count = 4, ranges = [(2, 3), (0, 1)], has_lightweight_delete = 0)),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

-- Every disk argument must be a `key = value` pair with both the key and the value present.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(equals(type)),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }
