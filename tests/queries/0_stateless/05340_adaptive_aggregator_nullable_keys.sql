-- The adaptive aggregator takes Nullable keys whose method packs the null map into the key. Every key
-- has NULL next to its nested default value, and every result is compared with the ordinary path.

SET max_threads = 4;
SET max_block_size = 1024;
SET adaptive_aggregator_freeze_threshold = 8;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET log_queries = 1;
SET log_profile_events = 1;

-- nullable_keys128
SELECT 'packed',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_packed';

-- nullable_keys128 with count() only
SELECT 'packed_count',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_packed_count';

-- nullable_keys128_void
SELECT 'packed_keys_only',
    (SELECT count(), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0))) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0))) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_packed_keys_only';

-- nullable_keys256
SELECT 'packed_wide',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(d), ifNull(d, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, toUInt64(number % 1000)) AS a, if(number % 13 = 0, NULL, toUInt64(number % 3)) AS d,
            if(number % 11 = 0, NULL, toUInt64(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, d, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(d), ifNull(d, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, toUInt64(number % 1000)) AS a, if(number % 13 = 0, NULL, toUInt64(number % 3)) AS d,
            if(number % 11 = 0, NULL, toUInt64(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, d, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_packed_wide';

-- nullable_prealloc_serialized
SELECT 'string_number',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, ''), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 13 = 0, NULL, if(number % 17 = 0, '', toString(number % 3000))) AS a, if(number % 5 = 0, NULL, number % 7) AS b,
            count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, ''), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 13 = 0, NULL, if(number % 17 = 0, '', toString(number % 3000))) AS a, if(number % 5 = 0, NULL, number % 7) AS b,
            count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_string_number';

-- nullable_serialized
SELECT 'array_number',
    (SELECT count(), sum(c), sum(cityHash64(arr, isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT range(number % 4) AS arr, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY arr, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(arr, isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT range(number % 4) AS arr, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY arr, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_array_number';

-- nullable_keys256 with a single key
SELECT 'single_wide',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, toInt128(number % 100000)) AS a, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, toInt128(number % 100000)) AS a, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_single_wide';

-- nullable_keys128 with external aggregation
SELECT 'external',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 1)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1)
SETTINGS log_comment = '05340_arm_external';

-- nullable_keys128 under ROLLUP
SELECT 'rollup',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY ROLLUP(a, b)) SETTINGS enable_adaptive_aggregator = 0, group_by_use_nulls = 1)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, number % 1000) AS a, if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY ROLLUP(a, b)) SETTINGS enable_adaptive_aggregator = 1, group_by_use_nulls = 1)
SETTINGS log_comment = '05340_arm_rollup';

-- nullable_key64 keeps NULL in a separate cell and stays on the ordinary path
SELECT 'single',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, toUInt64(number % 100000)) AS a, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, 0), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, toUInt64(number % 100000)) AS a, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_single';

-- nullable_key_string keeps NULL in a separate cell and stays on the ordinary path
SELECT 'single_string',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, ''), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, if(number % 13 = 0, '', toString(number % 100000))) AS a, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, ''), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, if(number % 13 = 0, '', toString(number % 100000))) AS a, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_single_string';

-- nullable_key_fixed_string keeps NULL in a separate cell and stays on the ordinary path
SELECT 'single_fixed_string',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, toFixedString('', 6)), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, if(number % 13 = 0, toFixedString('', 6), toFixedString(toString(number % 100000), 6))) AS a,
            count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, toFixedString('', 6)), c, s)) FROM (
        SELECT if(number % 7 = 0, NULL, if(number % 13 = 0, toFixedString('', 6), toFixedString(toString(number % 100000), 6))) AS a,
            count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_single_fixed_string';

-- A LowCardinality key is rejected by type, although this shape's method is nullable_prealloc_serialized
SELECT 'low_cardinality',
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, ''), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT toLowCardinality(if(number % 7 = 0, NULL, if(number % 13 = 0, '', toString(number % 3000)))) AS a,
            if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 0)
    =
    (SELECT count(), sum(c), sum(cityHash64(isNull(a), ifNull(a, ''), isNull(b), ifNull(b, 0), c, s)) FROM (
        SELECT toLowCardinality(if(number % 7 = 0, NULL, if(number % 13 = 0, '', toString(number % 3000)))) AS a,
            if(number % 11 = 0, NULL, toUInt32(intDiv(number, 3) % 500)) AS b, count() AS c, sum(number) AS s
        FROM numbers_mt(300000) GROUP BY a, b) SETTINGS enable_adaptive_aggregator = 1)
SETTINGS log_comment = '05340_arm_low_cardinality';

SYSTEM FLUSH LOGS query_log;
SELECT replaceOne(log_comment, '05340_arm_', ''), ProfileEvents['AdaptiveAggregationStagedRecords'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND log_comment LIKE '05340_arm_%'
ORDER BY log_comment;
