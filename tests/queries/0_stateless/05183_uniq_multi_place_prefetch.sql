-- Compare the multi-place path with the single-place path on the same ordered inputs.
-- `uniq` can serialize the same set in a different order after batched single-place insertion.
SET max_threads = 1;
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;
SET max_block_size = 1009;

CREATE TABLE uniq_multi_place
(
    g UInt64, v UInt64,
    i Int64 ALIAS -toInt64(v),
    f Float64 ALIAS if(v % 2, toFloat64(v), -toFloat64(v)),
    s String ALIAS toString(v),
    fs FixedString(16) ALIAS toFixedString(toString(v), 16),
    u128 UInt128 ALIAS toUInt128(v) * toUInt128('18446744073709551617'),
    ip IPv6 ALIAS toIPv6(concat('::ffff:0.0.0.', toString(v % 256)))
) ENGINE = Memory;

-- Cover small sets, table growth, and both transitions in `uniqCombined`.
INSERT INTO uniq_multi_place
SELECT number % 67, intDiv(number, 67) % arrayElement([1, 16, 17, 128, 129, 256, 513], number % 7 + 1)
FROM numbers(67 * 1025 + 19);

CREATE VIEW uniq_multi_place_check AS
SELECT countIf(NOT ok0), countIf(NOT ok1), countIf(NOT ok2), countIf(NOT ok3), countIf(NOT ok4)
FROM
(
    SELECT
        uniq(x) = arrayReduce('uniq', groupArray(x)) AS ok0,
        hex(uniqCombinedState(x)) = hex(arrayReduce('uniqCombinedState', groupArray(x))) AS ok1,
        hex(uniqCombined64State(x)) = hex(arrayReduce('uniqCombined64State', groupArray(x))) AS ok2,
        hex(uniqCombinedState(12)(x)) = hex(arrayReduce('uniqCombinedState(12)', groupArray(x))) AS ok3,
        hex(uniqCombined64State(12)(x)) = hex(arrayReduce('uniqCombined64State(12)', groupArray(x))) AS ok4
    FROM (SELECT g, {value:Identifier} AS x FROM uniq_multi_place)
    GROUP BY g
);

SELECT 'UInt64', * FROM uniq_multi_place_check(value = 'v');
SELECT 'Int64', * FROM uniq_multi_place_check(value = 'i');
SELECT 'Float64', * FROM uniq_multi_place_check(value = 'f');
SELECT 'String', * FROM uniq_multi_place_check(value = 's');
SELECT 'FixedString', * FROM uniq_multi_place_check(value = 'fs');
SELECT 'UInt128', * FROM uniq_multi_place_check(value = 'u128');
SELECT 'IPv6', * FROM uniq_multi_place_check(value = 'ip');

-- Preserve conditional filtering and the offset of multiple aggregate states.
SELECT 'If',
    countIf(finalizeAggregation(s1) != arrayReduce('uniq', a)),
    countIf(hex(s2) != hex(arrayReduce('uniqCombinedState', a))),
    countIf(hex(s3) != hex(arrayReduce('uniqCombined64State', a)))
FROM
(
    SELECT
        uniqIfState(v, v % 3 != 0) AS s1,
        uniqCombinedIfState(v, v % 3 != 0) AS s2,
        uniqCombined64IfState(v, v % 3 != 0) AS s3,
        groupArrayIf(v, v % 3 != 0) AS a
    FROM uniq_multi_place GROUP BY g
);

SELECT 'Nullable', countIf(NOT ok0), countIf(NOT ok1), countIf(NOT ok2), countIf(NOT ok3), countIf(NOT ok4)
FROM
(
    SELECT
        uniq(x) = arrayReduce('uniq', groupArray(x)) AS ok0,
        uniqCombined(x) = arrayReduce('uniqCombined', groupArray(x)) AS ok1,
        uniqCombined64(x) = arrayReduce('uniqCombined64', groupArray(x)) AS ok2,
        uniqCombined(12)(x) = arrayReduce('uniqCombined(12)', groupArray(x)) AS ok3,
        uniqCombined64(12)(x) = arrayReduce('uniqCombined64(12)', groupArray(x)) AS ok4
    FROM (SELECT g, if(v % 3, v, NULL) AS x FROM uniq_multi_place)
    GROUP BY g
);

TRUNCATE TABLE uniq_multi_place;

-- Cross the default HyperLogLog thresholds and the thinning threshold in `uniq`.
INSERT INTO uniq_multi_place SELECT number % 8, intDiv(number, 8) FROM numbers(8 * 140001);

SELECT 'Large', * FROM uniq_multi_place_check(value = 'v');

DROP VIEW uniq_multi_place_check;
DROP TABLE uniq_multi_place;

-- Overflow groups produce null places interleaved with retained groups.
SELECT 'Skipped places', min(u), max(u), min(c), max(c), min(c64), max(c64), max(empty)
FROM
(
    SELECT number % 67 AS g,
        uniq(intDiv(number, 67)) AS u,
        uniqCombinedIf(intDiv(number, 67), number % 3 != 0) AS c,
        uniqCombined64If(intDiv(number, 67), number % 3 != 0) AS c64,
        uniqIf(number, g = 1000) AS empty
    FROM numbers(67 * 129)
    GROUP BY g
    SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'any', max_block_size = 17
);
