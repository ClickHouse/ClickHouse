-- Isolate from the default ratio threshold: the spill must be triggered only by the explicit settings.
SET max_bytes_ratio_before_external_distinct = 0;

-- The first plan records the `ORDER BY` requirement on the final `DISTINCT`. In the second plan, the
-- subquery's expression ordering does not establish an input-order requirement for the outer `DISTINCT`.
SELECT count() FROM (EXPLAIN PLAN actions = 1 SELECT DISTINCT number AS a FROM numbers(10) ORDER BY a + 1 DESC) WHERE explain LIKE '%Preserve input order%';
SELECT count() FROM (EXPLAIN PLAN actions = 1 SELECT DISTINCT a FROM (SELECT number AS a FROM numbers(10) ORDER BY a + 1) SETTINGS query_plan_remove_redundant_sorting = 0) WHERE explain LIKE '%Preserve input order%';

-- The final `DISTINCT` of a query with `ORDER BY` runs above the sort, so it must return the rows in the
-- sorted order also when it spills: the spilled rows are merged in `DISTINCT`-key order and then sorted
-- back into their arrival order. The sort is on an expression of the key, so the sorted-prefix `DISTINCT`
-- does not apply and the spilling transform is used. The 1-byte threshold (with exact memory tracking and a
-- pinned block size) makes the spill deterministic. The `DISTINCT` output is a single stream and so is the
-- aggregation over it, so `groupArray` observes the arrival order.
SELECT count(), uniqExact(a), groupArray(a) = arrayReverseSort(groupArray(a)) FROM (SELECT DISTINCT number AS a FROM numbers(300000) ORDER BY a + 1 DESC) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0, log_comment = '05059_external_distinct_order_by/spill';

-- The same through a remote server that receives the serialized query plan: the single shard executes
-- the whole query, so the order requirement has to travel with the plan. The local replica must not be
-- preferred, otherwise the query would run in-process without the round trip.
SELECT count(), uniqExact(a), groupArray(a) = arrayReverseSort(groupArray(a)) FROM (SELECT DISTINCT number AS a FROM remote('127.0.0.1', view(SELECT number FROM numbers(300000))) ORDER BY a + 1 DESC) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0, serialize_query_plan = 1, prefer_localhost_replica = 0, log_comment = '05059_external_distinct_order_by/remote';

-- With a `LIMIT` larger than the rows emitted before the spill (the first block), the result is the head of
-- the sorted order.
SELECT count(), min(a), max(a) FROM (SELECT DISTINCT number % 300000 AS a FROM numbers(600000) ORDER BY a + 1 DESC LIMIT 100000) SETTINGS max_bytes_before_external_distinct = 1, max_block_size = 65409, max_untracked_memory = 0;

-- Both queries spill, with the serialized plan performing external `DISTINCT` on the remote server.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ExternalDistinctWritePart'] > 0, ProfileEvents['ExternalDistinctMerge']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment = '05059_external_distinct_order_by/spill';
SELECT count(),
       countIf(ProfileEvents['ExternalDistinctWritePart'] > 0 AND ProfileEvents['ExternalDistinctMerge'] > 0)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND NOT is_initial_query AND log_comment = '05059_external_distinct_order_by/remote'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
            AND is_initial_query AND current_database = currentDatabase()
            AND log_comment = '05059_external_distinct_order_by/remote');
