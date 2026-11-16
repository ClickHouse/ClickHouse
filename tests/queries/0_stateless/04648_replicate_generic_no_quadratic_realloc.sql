-- A constant Array(JSON) argument to an aggregate reaches ColumnArray::replicateGeneric, which appended
-- shared data one row at a time and re-reserved the whole nested column each time, so the bytes copied
-- grew quadratically. max_execution_time only guards a return; the runner's own timeout trips first.

SET enable_analyzer = 1;
SET max_block_size = 400000;
SET max_threads = 1;

SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('a', 1)::JSON] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('a', 1)::JSON, map('b', 2)::JSON, map('c', 3)::JSON] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
-- The fix is in insertManyDefaults, below every wrapper, so a carrier at any depth is covered: nested
-- Array, Map values, and Variant or Dynamic payloads all reach it. Measured 4.3x to 5.9x at 3.2M rows.
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [[map('a', 1)::JSON]] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [[[map('a', 1)::JSON]]] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [tuple(map('a', 1)::JSON, 1)] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('a', 1)::JSON::Nullable(JSON)] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('k', map('a', 1)::JSON)] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('a', 1)::JSON::Dynamic] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('a', 1)::JSON::Variant(UInt64, JSON)] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [1::Dynamic, 'x'::Dynamic] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [1::Variant(UInt64, String)] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [map('a', 1)] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [toLowCardinality('x')] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_execution_time = 60;

-- Appending offsets must not raise peak memory, since IColumn::reserve affects performance only. These
-- pass from the same limit as before the fix; an object with no shared-data pair is the tight case,
-- because it is the shape whose row count says least about how much nested data is really appended.
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [CAST('{}', 'JSON(max_dynamic_paths=0)')] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_memory_usage = 20000000;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [CAST([], 'Array(FixedString(256))')] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_memory_usage = 20000000;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [CAST([[]], 'Array(Array(FixedString(256)))')] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_memory_usage = 20000000;
SELECT any(c) IS NOT NULL, count() FROM (SELECT materialize(1) AS k, [CAST('{}', 'JSON(a Array(FixedString(256)))')] AS c FROM numbers(400000)) GROUP BY k SETTINGS max_memory_usage = 20000000;

-- replicateGeneric must still produce the right values, not just produce them quickly. The wrapper
-- shapes are checked by content here, since the aggregates above only assert a row count.
SELECT arrayJoin(c) FROM (SELECT [map('k', map('a', 1)::JSON)] AS c FROM numbers(2)) ORDER BY 1;
SELECT arrayJoin(c) FROM (SELECT [map('a', 1)::JSON::Nullable(JSON), NULL::Nullable(JSON)] AS c FROM numbers(2)) ORDER BY 1 NULLS LAST;
SELECT toString(arrayJoin(c)) FROM (SELECT [map('a', 1)::JSON::Dynamic] AS c FROM numbers(2)) ORDER BY 1;
SELECT toString(arrayJoin(c)) FROM (SELECT [map('a', 1)::JSON::Variant(UInt64, JSON)] AS c FROM numbers(2)) ORDER BY 1;
SELECT arrayJoin(c) FROM (SELECT [map('a', 1)::JSON, map('b', 2)::JSON] AS c FROM numbers(2)) ORDER BY 1;
SELECT arrayJoin(arrayJoin(c)) FROM (SELECT [[map('a', 1)::JSON]] AS c FROM numbers(2)) ORDER BY 1;
SELECT length(c), c[1], c[2] FROM (SELECT [1::Dynamic, 'x'::Dynamic] AS c FROM numbers(2));
SELECT groupArray(length(c)) FROM (SELECT materialize(1) AS k, [map('a', 1)::JSON, map('b', 2)::JSON, map('c', 3)::JSON] AS c FROM numbers(3)) GROUP BY k;
