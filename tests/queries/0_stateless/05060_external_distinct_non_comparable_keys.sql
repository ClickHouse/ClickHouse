-- Force a spill on every chunk: the query memory usage always exceeds one byte (the untracked memory is
-- disabled so that small queries appear to use memory at all).
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;

-- A key column whose type supports only equality checks (an aggregate state) cannot be sorted, so the
-- spilled runs carry its serialized values and compare them as bytes: equal states have equal
-- serializations. Ten thousand states over one thousand distinct values, ten copies of each (the state
-- of a group is determined by x alone).
CREATE VIEW states AS SELECT sumState(x) AS s, x, g FROM (SELECT number % 1000 AS x, intDiv(number, 1000) AS g FROM numbers(10000)) GROUP BY x, g;

SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT s FROM states) WHERE explain LIKE '%ExternalDistinctTransform%' SETTINGS log_comment = '05060_external_distinct_non_comparable_keys/plan';

SELECT count() FROM (SELECT DISTINCT s FROM states) SETTINGS log_comment = '05060_external_distinct_non_comparable_keys/state_count';
SELECT sum(finalizeAggregation(s)), min(finalizeAggregation(s)), max(finalizeAggregation(s)) FROM (SELECT DISTINCT s FROM states) SETTINGS log_comment = '05060_external_distinct_non_comparable_keys/state_values';

-- A serialized state key can be combined with a comparable key.
SELECT count() FROM (SELECT DISTINCT s, g % 2 AS parity FROM states) SETTINGS log_comment = '05060_external_distinct_non_comparable_keys/mixed_keys';

-- The `DISTINCT` after an `ORDER BY` keeps the sorted order across the spill also with a serialized key
-- (the sort is by an expression, so that the `DISTINCT` is the hash-based one above the sort, not the
-- in-order one).
SELECT count(), groupArray(x) = arraySort(groupArray(x)) FROM (SELECT x FROM (SELECT DISTINCT s, x FROM states ORDER BY x + 1)) SETTINGS max_threads = 1, log_comment = '05060_external_distinct_non_comparable_keys/ordered';

-- Sets of one to four elements produce serializations of different lengths.
SELECT count(), arraySort(groupArray(finalizeAggregation(u)))
FROM (SELECT DISTINCT u FROM (SELECT uniqExactState(v) AS u FROM (SELECT number % 40 AS x, intDiv(number, 40) AS g, arrayJoin(range(1 + (number % 40) % 4)) AS v FROM numbers(4000)) GROUP BY x, g)) SETTINGS log_comment = '05060_external_distinct_non_comparable_keys/variable_length';

-- Disabling spilling produces the same results.
SELECT count() FROM (SELECT DISTINCT s FROM states) SETTINGS max_bytes_before_external_distinct = 0, log_comment = '05060_external_distinct_non_comparable_keys/disabled_count';
SELECT sum(finalizeAggregation(s)), min(finalizeAggregation(s)), max(finalizeAggregation(s)) FROM (SELECT DISTINCT s FROM states) SETTINGS max_bytes_before_external_distinct = 0, log_comment = '05060_external_distinct_non_comparable_keys/disabled_values';
SELECT count() FROM (SELECT DISTINCT s, g % 2 AS parity FROM states) SETTINGS max_bytes_before_external_distinct = 0, log_comment = '05060_external_distinct_non_comparable_keys/disabled_mixed_keys';

-- Each execution records whether it spilled. The disabled controls and pipeline introspection keep
-- both external-processing counters at zero.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05060_external_distinct_non_comparable_keys/') + 1) AS test_case,
    min(ProfileEvents['ExternalDistinctWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalDistinctMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05060_external_distinct_non_comparable_keys/')
GROUP BY log_comment
ORDER BY test_case;

DROP VIEW states;
