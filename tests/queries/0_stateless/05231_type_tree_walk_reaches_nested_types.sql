-- The `Variant`/`Dynamic`/`JSON` refusals walk the whole type tree rather than looking at the
-- outermost type, so they have to reach the type wherever a container can hide it: an `Array`
-- element, a `Tuple` element, a `Map` key, a `Map` value, and at any depth of nesting.

SET allow_suspicious_types_in_group_by = 0, allow_suspicious_types_in_order_by = 0;

SELECT count() FROM numbers(2) GROUP BY CAST([number], 'Array(Variant(UInt64, String))'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM numbers(2) GROUP BY CAST((number, number), 'Tuple(UInt64, Dynamic)'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM numbers(2) GROUP BY CAST(map('k', number), 'Map(String, Variant(UInt64, String))'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM numbers(2) GROUP BY CAST(map(number, number), 'Map(Variant(UInt64, String), UInt64)'); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM numbers(2) GROUP BY CAST([[[number]]], 'Array(Array(Array(Dynamic)))'); -- { serverError ILLEGAL_COLUMN }
SELECT number FROM numbers(2) ORDER BY CAST([number], 'Array(Variant(UInt64, String))'); -- { serverError ILLEGAL_COLUMN }

-- A key expression refuses `JSON` as well, which `GROUP BY` does not, and walks the tree the same way.
DROP TABLE IF EXISTS t_nested_key;
CREATE TABLE t_nested_key (c Array(Variant(UInt64, String))) ENGINE = MergeTree ORDER BY c; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }
CREATE TABLE t_nested_key (c Map(String, Array(JSON))) ENGINE = MergeTree ORDER BY c; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_KEY }

-- A container that hides nothing suspicious is still accepted, at any depth and behind any wrapper.
SELECT count() FROM (SELECT count() FROM numbers(2) GROUP BY CAST([number], 'Array(Nullable(UInt64))'));
SELECT count() FROM (SELECT count() FROM numbers(2) GROUP BY CAST([toString(number)], 'Array(LowCardinality(String))'));
SELECT count() FROM (SELECT count() FROM numbers(2) GROUP BY CAST(map(number, [number]), 'Map(UInt64, Array(Nullable(UInt64)))'));
SELECT count() FROM (SELECT count() FROM numbers(2) GROUP BY CAST([concat('{"a":', toString(number), '}')], 'Array(JSON)'));
