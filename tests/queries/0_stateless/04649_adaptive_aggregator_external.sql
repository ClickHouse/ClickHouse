-- Tags: long

-- Exercises the adaptive aggregator under memory pressure: past the external-aggregation
-- threshold the staged backlogs are drained early into the shared routing table, and the
-- routing table spills through the ordinary external-aggregation machinery. A threshold of one
-- byte keeps the pressure valve firing for the whole query, so every drained record takes the
-- persist-key path and the merge goes external. Every cell compares the same query with the
-- feature off (and no forced spilling) and on.

SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;

SELECT 'Count-only value staging under constant pressure';
SELECT
    (SELECT count(), sum(c) FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c) FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

SELECT 'General aggregates under constant pressure';
SELECT
    (SELECT count(), sum(s), sum(mn) FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s), sum(mn) FROM (SELECT number % 100000 AS g, sum(number) AS s, min(number) AS mn FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

SELECT 'String keys persist into the routing table under pressure';
SELECT
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 100000)) AS k, count() AS c FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 100000)) AS k, count() AS c FROM numbers_mt(400000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

SELECT 'Long string keys and a string argument under pressure';
SELECT
    (SELECT count(), sum(cityHash64(k)), sum(cityHash64(m)) FROM (SELECT repeat(toString(number % 50000), 5) AS k, max(repeat(toString(number), 7)) AS m FROM numbers_mt(200000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(cityHash64(k)), sum(cityHash64(m)) FROM (SELECT repeat(toString(number % 50000), 5) AS k, max(repeat(toString(number), 7)) AS m FROM numbers_mt(200000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

-- A moderate threshold engages the valve mid-query instead of constantly, so early-drained,
-- late-drained, and spilled data mix in one result.
SELECT 'Partial pressure mixes early and merge-time drains';
SELECT
    (SELECT count(), sum(s) FROM (SELECT number % 200000 AS g, sum(number) AS s FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(s) FROM (SELECT number % 200000 AS g, sum(number) AS s FROM numbers_mt(600000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 20000000));

-- Fat states with few groups take the give-up path and spill through the ordinary baseline
-- branch; the finish path must still merge them with the adaptive state externally.
SELECT 'Give-up threads spill through the baseline branch';
SELECT
    (SELECT count(), sum(u) FROM (SELECT number % 50 AS g, uniqExact(number) AS u FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(u) FROM (SELECT number % 50 AS g, uniqExact(number) AS u FROM numbers_mt(400000) GROUP BY g SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1));

-- A key freeze threshold above both the key count and the give-up bound, with the byte bound
-- disabled, keeps every producer learning for the whole query, so the results mix tables that
-- stood down under pressure and spilled with tables that never crossed the threshold at all.
SELECT 'Learning-phase spill preserves results';
SELECT
    (SELECT count(), sum(c) FROM (SELECT concat(toString(number), repeat('x', number % 40)) AS k, count() AS c FROM numbers_mt(3000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(c) FROM (SELECT concat(toString(number), repeat('x', number % 40)) AS k, count() AS c FROM numbers_mt(3000000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 4000000, adaptive_aggregator_freeze_threshold_bytes = 0, group_by_two_level_threshold = 1000, max_bytes_before_external_group_by = 20000000, max_bytes_ratio_before_external_group_by = 0));

-- Few distinct keys spread over many routing buckets leave each bucket holding only a handful
-- of records, and small blocks mean a bucket's first block often carries none of them. Both
-- string key layouts are compared, because each pre-sizes its table differently.
SELECT 'Sparsely populated buckets under pressure';
SELECT
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 700)) AS k, count() AS c FROM numbers_mt(20000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0))
    =
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 700)) AS k, count() AS c FROM numbers_mt(20000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 8, group_by_two_level_threshold = 1, max_block_size = 64, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0));
SELECT
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 700)) AS k, count() AS c FROM numbers_mt(20000) GROUP BY k SETTINGS enable_adaptive_aggregator = 0, enable_packed_string_keys_in_aggregation = 0))
    =
    (SELECT count(), sum(cityHash64(k)), sum(c) FROM (SELECT concat('key_', toString(number % 700)) AS k, count() AS c FROM numbers_mt(20000) GROUP BY k SETTINGS enable_adaptive_aggregator = 1, enable_packed_string_keys_in_aggregation = 0, adaptive_aggregator_freeze_threshold = 8, group_by_two_level_threshold = 1, max_block_size = 64, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0));

SELECT 'Analytic guard under pressure';
SELECT count(), sum(g), sum(s), sum(c) FROM (SELECT number % 30000 AS g, sum(number) AS s, count() AS c FROM numbers_mt(120000) GROUP BY g)
SETTINGS enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1;
