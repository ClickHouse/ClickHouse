-- uniqExact with GROUP BY, where two groups get many distinct values and the others few,
-- compared with groupUniqArray over the same rows.

DROP VIEW IF EXISTS v_uniq_exact_multi_place;

-- Two large groups interleaved with small ones and with groups that keep appearing, then a tail with small groups only.
CREATE VIEW v_uniq_exact_multi_place AS
SELECT
    multiIf(number >= 800000, 2 + number % 61, number % 4 < 2, number % 4, number % 4 = 2, 2 + intDiv(number, 4) % 61, 1000 + intDiv(number, 4000)) AS g,
    multiIf(number >= 800000, number % 97, number % 4 < 2, intHash64(number), number % 4 = 2, number % 97, number % 7) AS v
FROM numbers(900000);

SET max_block_size = 10000, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'single level', count(), countIf(u != a), sum(u)
FROM (SELECT g, uniqExact(v) AS u, length(groupUniqArray(v)) AS a FROM v_uniq_exact_multi_place GROUP BY g)
SETTINGS max_threads = 1, log_comment = 'uniq_exact_prefetch_single_level';

SELECT 'two level', count(), countIf(u != a), sum(u)
FROM (SELECT g, uniqExact(v) AS u, length(groupUniqArray(v)) AS a FROM v_uniq_exact_multi_place GROUP BY g)
SETTINGS max_threads = 2, log_comment = 'uniq_exact_prefetch_two_level';

SELECT 'String', count(), countIf(u != a), sum(u)
FROM (SELECT g, uniqExact(toString(v)) AS u, length(groupUniqArray(toString(v))) AS a FROM v_uniq_exact_multi_place GROUP BY g)
SETTINGS max_threads = 2, log_comment = 'uniq_exact_prefetch_string';

SELECT 'If', count(), countIf(u != a), sum(u)
FROM (SELECT g, uniqExactIf(v, v % 3 != 0) AS u, length(groupUniqArrayIf(v, v % 3 != 0)) AS a FROM v_uniq_exact_multi_place GROUP BY g)
SETTINGS max_threads = 2, log_comment = 'uniq_exact_prefetch_if';

SELECT 'Small sets', count(), countIf(u != a), sum(u)
FROM (SELECT g, uniqExact(v) AS u, length(groupUniqArray(v)) AS a FROM v_uniq_exact_multi_place WHERE g >= 2 GROUP BY g)
SETTINGS max_threads = 2, log_comment = 'uniq_exact_prefetch_small_sets';

-- Rows of groups that appear after the limit on the number of groups is reached are skipped.
SELECT 'Skipped rows', countIf(u != a)
FROM (SELECT g, uniqExact(v) AS u, length(groupUniqArray(v)) AS a FROM v_uniq_exact_multi_place GROUP BY g)
SETTINGS max_threads = 1, max_rows_to_group_by = 100, group_by_overflow_mode = 'any';

-- Large sets take the prefetching path, sets of at most 97 values do not.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationUniqExactPrefetchedRows'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND startsWith(log_comment, 'uniq_exact_prefetch_')
ORDER BY log_comment;

DROP VIEW v_uniq_exact_multi_place;
