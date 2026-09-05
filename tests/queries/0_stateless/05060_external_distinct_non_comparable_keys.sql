-- Force a spill on every chunk: the query memory usage always exceeds one byte (the untracked memory is
-- disabled so that small queries appear to use memory at all).
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;
SET log_comment = '05060_external_distinct_non_comparable_keys';

-- A key column whose type supports only equality checks (an aggregate state) cannot be sorted, so the
-- spilled runs carry its serialized values and compare them as bytes: equal states have equal
-- serializations. Ten thousand states over one thousand distinct values, ten copies of each (the state
-- of a group is determined by x alone).
CREATE VIEW states AS SELECT sumState(x) AS s, x, g FROM (SELECT number % 1000 AS x, intDiv(number, 1000) AS g FROM numbers(10000)) GROUP BY x, g;

SELECT count() FROM (EXPLAIN PIPELINE SELECT DISTINCT s FROM states) WHERE explain LIKE '%ExternalDistinctTransform%';

SELECT count() FROM (SELECT DISTINCT s FROM states);
SELECT sum(finalizeAggregation(s)), min(finalizeAggregation(s)), max(finalizeAggregation(s)) FROM (SELECT DISTINCT s FROM states);

-- The state key mixed with a comparable key.
SELECT count() FROM (SELECT DISTINCT s, g % 2 AS parity FROM states);

-- The DISTINCT after an ORDER BY keeps the sorted order across the spill also with a serialized key (the
-- sort is by an expression, so that the DISTINCT is the hash-based one above the sort, not the in-order one).
SELECT count(), groupArray(x) = arraySort(groupArray(x)) FROM (SELECT x FROM (SELECT DISTINCT s, x FROM states ORDER BY x + 1)) SETTINGS max_threads = 1;

-- States whose serializations differ in length (sets of one to four elements).
SELECT count(), arraySort(groupArray(finalizeAggregation(u)))
FROM (SELECT DISTINCT u FROM (SELECT uniqExactState(v) AS u FROM (SELECT number % 40 AS x, intDiv(number, 40) AS g, arrayJoin(range(1 + (number % 40) % 4)) AS v FROM numbers(4000)) GROUP BY x, g));

-- The same results without the spill.
SELECT count() FROM (SELECT DISTINCT s FROM states) SETTINGS max_bytes_before_external_distinct = 0;
SELECT sum(finalizeAggregation(s)), min(finalizeAggregation(s)), max(finalizeAggregation(s)) FROM (SELECT DISTINCT s FROM states) SETTINGS max_bytes_before_external_distinct = 0;
SELECT count() FROM (SELECT DISTINCT s, g % 2 AS parity FROM states) SETTINGS max_bytes_before_external_distinct = 0;

-- The spilling queries above (all the DISTINCT queries except the EXPLAIN and the ones with the spill
-- disabled) must actually have written temporary files.
SYSTEM FLUSH LOGS query_log;
SELECT countIf(ProfileEvents['ExternalDistinctWritePart'] >= 1) >= 5
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05060_external_distinct_non_comparable_keys'
    AND query LIKE '%SELECT DISTINCT%' AND query NOT LIKE '%EXPLAIN%' AND query NOT LIKE '%max_bytes_before_external_distinct = 0%';

DROP VIEW states;
