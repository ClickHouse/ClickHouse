-- Tags: no-random-settings
-- `max_threads = 4` requires at least four final leaf partitions, while the per-pass fanout cap of
-- two forces a 1+1-bit plan and exercises the refine scatter for both simple and composite keys.
-- Plan arithmetic: fanout = bit_ceil(max_threads) = 4 (the tiny build stays within the leaf byte
-- budget), `radix_join_max_partitions_per_pass = 2` caps every pass at 1 bit, so `computePassBits`
-- yields [1, 1] — two radix passes. The keys are cast to `UInt64` (packed key widths 8 and 8+8)
-- because the planner gate `radixHashJoinApplicable` rejects the narrow `UInt8`/`UInt16` keys that
-- `number % k` would produce, silently falling back to `parallel_hash` and making the test vacuous.
SET enable_analyzer = 1;
SET max_threads = 4;
SET radix_join_max_partitions_per_pass = 2;

SELECT
    'single_u64',
    (
        SELECT (count(), sum(cityHash64(p.probe_payload, b.build_payload)))
        FROM
            (SELECT number AS probe_payload, toUInt64(number % 150) AS key FROM numbers(400)) AS p
        INNER JOIN
            (SELECT number AS build_payload, toUInt64(number % 100) AS key FROM numbers(600)) AS b
        ON p.key = b.key
        SETTINGS join_algorithm = 'radix_join'
    )
    =
    (
        SELECT (count(), sum(cityHash64(p.probe_payload, b.build_payload)))
        FROM
            (SELECT number AS probe_payload, toUInt64(number % 150) AS key FROM numbers(400)) AS p
        INNER JOIN
            (SELECT number AS build_payload, toUInt64(number % 100) AS key FROM numbers(600)) AS b
        ON p.key = b.key
        SETTINGS join_algorithm = 'hash'
    )
SETTINGS log_comment = '04511_radix_multi_pass_single_u64';

SELECT
    'composite_u64',
    (
        SELECT (count(), sum(cityHash64(p.probe_payload, b.build_payload)))
        FROM
            (SELECT number AS probe_payload, toUInt64(number % 150) AS key1, toUInt64(number % 17) AS key2 FROM numbers(400)) AS p
        INNER JOIN
            (SELECT number AS build_payload, toUInt64(number % 100) AS key1, toUInt64(number % 17) AS key2 FROM numbers(600)) AS b
        ON p.key1 = b.key1 AND p.key2 = b.key2
        SETTINGS join_algorithm = 'radix_join'
    )
    =
    (
        SELECT (count(), sum(cityHash64(p.probe_payload, b.build_payload)))
        FROM
            (SELECT number AS probe_payload, toUInt64(number % 150) AS key1, toUInt64(number % 17) AS key2 FROM numbers(400)) AS p
        INNER JOIN
            (SELECT number AS build_payload, toUInt64(number % 100) AS key1, toUInt64(number % 17) AS key2 FROM numbers(600)) AS b
        ON p.key1 = b.key1 AND p.key2 = b.key2
        SETTINGS join_algorithm = 'hash'
    )
SETTINGS log_comment = '04511_radix_multi_pass_composite_u64';

-- Engagement assertions: the queries above must actually run on the radix path. A silent fallback
-- (e.g. a future planner-gate change re-rejecting these keys) produces zero `RadixHashJoin*`
-- profile events and fails here, instead of leaving the comparisons vacuously green. With the
-- settings above the plan is fanout 4 in 2 passes and all four leaf partitions are non-empty
-- (100 distinct build keys spread over 4 partitions), so `RadixHashJoinLeafGroupBuilds` is exactly
-- 4 for the radix half of each comparison (the `hash` half contributes zero).
SYSTEM FLUSH LOGS query_log;

SELECT
    'single_u64_engaged',
    ProfileEvents['RadixHashJoinLeafGroupBuilds']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04511_radix_multi_pass_single_u64'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT
    'composite_u64_engaged',
    ProfileEvents['RadixHashJoinLeafGroupBuilds']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment = '04511_radix_multi_pass_composite_u64'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;
