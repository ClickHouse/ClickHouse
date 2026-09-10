-- Force a spill on every chunk: the query memory usage always exceeds one byte. The threshold is
-- compared with the memory tracked by the query, so allocations must not linger in the thread-local
-- untracked buffers, otherwise small queries never appear to use any memory.
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;

SELECT count() FROM (SELECT DISTINCT toString(number % 1000) AS k FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/string';
SELECT count() FROM (SELECT DISTINCT toFixedString(toString(number % 100), 8) AS k FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/fixed_string';
SELECT count() FROM (SELECT DISTINCT if(number % 10 = 0, NULL, toString(number % 100)) AS k FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/nullable_string';
SELECT count() FROM (SELECT DISTINCT if(number % 10 = 0, NULL, number % 100) AS k FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/nullable_number';
SELECT count() FROM (SELECT DISTINCT toInt128(number % 50) AS k FROM numbers(1000)) SETTINGS log_comment = '04494_external_distinct_key_types/int128';
SELECT count() FROM (SELECT DISTINCT toLowCardinality(toString(number % 100)) AS k FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/low_cardinality';
SELECT count() FROM (SELECT DISTINCT number % 10 AS a, number % 7 AS b FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/multiple_keys';
SELECT count() FROM (SELECT DISTINCT (number % 10, toString(number % 7)) AS t FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/tuple';
SELECT count() FROM (SELECT DISTINCT range(number % 5) AS a FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/array';

-- Values that compare equal in the sort order (0. and -0., NaNs with different payloads) may be
-- deduplicated as one value once the spill is involved - the same equality `DISTINCT` in order uses - while
-- a value class fully processed in memory keeps the binary distinction. Whatever the timing, the result
-- contains no binary duplicates and no value class is lost.
SELECT count() = uniqExact(reinterpretAsUInt64(k)), countIf(k = 0) BETWEEN 1 AND 2, countIf(isNaN(k)) BETWEEN 1 AND 2, count() BETWEEN 2 AND 4
FROM
(
    SELECT DISTINCT reinterpretAsFloat64(arrayJoin([toUInt64(0), 9223372036854775808, 0, 9223372036854775808, 9221120237041090560, 9221120237041090561]) + number * 0) AS k
    FROM numbers(3)
)
SETTINGS max_block_size = 2, log_comment = '04494_external_distinct_key_types/float_classes';

-- `DISTINCT` over constant columns only: there is nothing to spill, the in-memory `DISTINCT` is used.
SELECT DISTINCT 1, '1' FROM numbers(5) SETTINGS log_comment = '04494_external_distinct_key_types/constants';
SELECT DISTINCT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS log_comment = '04494_external_distinct_key_types/constant_limit';

-- A key type that supports only equality (no comparison) spills too: its values are compared as bytes.
SELECT count() FROM (SELECT DISTINCT s FROM (SELECT number % 3 AS g, uniqExactState(number) AS s FROM numbers(100) GROUP BY g)) SETTINGS log_comment = '04494_external_distinct_key_types/non_comparable';
SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT s FROM (SELECT number % 3 AS g, uniqExactState(number) AS s FROM numbers(100) GROUP BY g)) WHERE explain LIKE '%ExternalDistinctTransform%' SETTINGS log_comment = '04494_external_distinct_key_types/non_comparable_plan';

-- The flag column of the spilled runs must not clash with a user column of the same name (the old analyzer
-- keeps plain column names in the `DISTINCT` header).
SELECT count() FROM (SELECT DISTINCT number % 100 AS __distinct_already_emitted FROM numbers(1000)) SETTINGS enable_analyzer = 0, log_comment = '04494_external_distinct_key_types/service_name';

-- A constant non-key column alongside a real key: the first run rebuilds the constant from the header.
SELECT count(), sum(c) FROM (SELECT DISTINCT 7 AS c, number % 1000 AS k FROM numbers(10000)) SETTINGS log_comment = '04494_external_distinct_key_types/constant_payload';

-- Each key representation must exercise its expected execution path. Constant-only queries and
-- pipeline introspection do not spill; every other case uses external `DISTINCT`.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('04494_external_distinct_key_types/') + 1) AS test_case,
    min(ProfileEvents['ExternalDistinctWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalDistinctMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '04494_external_distinct_key_types/')
GROUP BY log_comment
ORDER BY test_case;
