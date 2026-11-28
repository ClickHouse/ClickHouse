-- Spill at the first chunk of every query: the query memory usage always exceeds one byte. The threshold is
-- compared with the memory tracked by the query, so allocations must not linger in the thread-local untracked
-- buffers.
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_bytes_before_external_limit_by = 1;
SET max_untracked_memory = 0;
SET max_block_size = 1000;

-- Without `ORDER BY` the input order has no meaning, so a spilling `LIMIT n OFFSET m BY` may keep any rows of
-- a key that fit its window `[m, m + n)`: as many as the in-memory one. The checks cover the number of rows,
-- the number of rows of each key through a checksum of the keys, and the absence of repeated rows.
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT number % 1000 AS k, number AS v FROM numbers(10000) LIMIT 2 OFFSET 1 BY k) SETTINGS log_comment = '05338_external_limit_by/number';

-- The `LIMIT BY` of a query with `ORDER BY` runs above the sort, so it keeps the first rows of each key in
-- the sorted order and returns them in that order also when it spills: the spilled rows are merged in key
-- order and then sorted back into their arrival order. The `LIMIT BY` output is a single stream and so is the
-- aggregation over it, so `groupArray` observes the order. The checksums of the rows equal those of the
-- in-memory `LIMIT BY`.
SELECT count(), groupArray(v) = arrayReverseSort(groupArray(v)), sum(cityHash64(k, v)) FROM (SELECT number % 1000 AS k, number AS v FROM numbers(10000) ORDER BY v DESC LIMIT 2 OFFSET 1 BY k) SETTINGS log_comment = '05338_external_limit_by/order_by';

-- Every local query above spilled and merged its runs.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05338_external_limit_by/') + 1) AS test_case,
    min(ProfileEvents['ExternalLimitByWritePart'] > 0) AS spilled,
    min(ProfileEvents['ExternalLimitByMerge'] > 0) AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05338_external_limit_by/')
GROUP BY log_comment
ORDER BY test_case;
