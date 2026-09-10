SET max_threads = 4;
SET max_block_size = 1000;
SET max_untracked_memory = 0;
SET max_bytes_ratio_before_external_distinct = 0;
SET max_bytes_before_external_distinct = 1;
SET allow_preliminary_distinct_abandoning = 0;
SET optimize_distinct_in_order = 1;

-- Preliminary hashing can release its set before sorting even when the final step deduplicates in order.
SELECT countIf(explain LIKE '%DistinctSortedStreamTransform%') > 0,
       countIf(explain LIKE '%ExternalDistinctTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 1000 AS k FROM numbers_mt(100000) ORDER BY k);

SELECT groupArray(k) = range(1000)
FROM (SELECT DISTINCT number % 1000 AS k FROM numbers_mt(100000) ORDER BY k)
SETTINGS log_comment = '05076_external_distinct_preliminary_policy/sorted';

-- A sorted prefix leaves the final step hashing the remaining keys within each equal-prefix range.
SELECT count(), uniqExact((p, k)), groupArray(p) = arraySort(groupArray(p))
FROM
(
    SELECT DISTINCT number % 2 AS p, intDiv(number, 2) % 1000 AS k
    FROM numbers_mt(100000) ORDER BY p
)
SETTINGS log_comment = '05076_external_distinct_preliminary_policy/prefix';

-- Reaching the hint takes priority over memory shedding in the same chunk.
SELECT count() > 0
FROM (EXPLAIN PLAN SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 10)
WHERE explain LIKE '%Distinct (Preliminary DISTINCT)%';

SELECT count()
FROM (SELECT DISTINCT number % 100 AS k FROM numbers_mt(100000) LIMIT 10)
SETTINGS log_comment = '05076_external_distinct_preliminary_policy/hint_first';

-- Shedding before the hint is reached leaves the final step responsible for exact deduplication.
SELECT count()
FROM (SELECT DISTINCT bitXor(intDiv(number, 100), 5) AS k FROM numbers_mt(100000) LIMIT 100)
SETTINGS log_comment = '05076_external_distinct_preliminary_policy/pressure_first';

SYSTEM FLUSH LOGS query_log;
SELECT splitByChar('/', log_comment)[-1],
       ProfileEvents['DistinctTransformsSwitchedToPassThrough'] > 0,
       ProfileEvents['ExternalDistinctWritePart'] > 0,
       read_rows < 100000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment LIKE '05076_external_distinct_preliminary_policy/%'
ORDER BY log_comment;
