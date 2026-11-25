-- Read-in-order state that lives outside `SelectQueryInfo` (`virtual_row_conversion`,
-- `prefer_multiple_streams`) must survive the rebuild of `ReadFromMergeTree` done by
-- `createLocalParallelReplicasReadingStep`: the local replica plan replaces every read step with a
-- freshly constructed one, and the constructor only carries `SelectQueryInfo`.
-- Pin that the read-in-order-through-join query returns the same rows with and without a local
-- parallel-replicas plan on a single-part table, i.e. the rebuilt read still produces the sorted
-- prefix the sort above the join relies on.

DROP TABLE IF EXISTS events_04657;
DROP TABLE IF EXISTS payloads_04657;

CREATE TABLE events_04657 (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time;
INSERT INTO events_04657 SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND, toString(number) FROM numbers(200000);
OPTIMIZE TABLE events_04657 FINAL;

CREATE TABLE payloads_04657 (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO payloads_04657 SELECT concat('Payload ', toString(number)), toString(number) FROM numbers(40) WHERE number % 4 = 0;

SET enable_analyzer = 1;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;
SET query_plan_optimize_join_order_limit = 1, query_plan_join_swap_table = 0;

SELECT 'without parallel replicas';
SELECT events_04657.Time, events_04657.Id, coalesce(nullIf(payloads_04657.Payload, ''), 'NULL') AS Payload
FROM events_04657 LEFT JOIN payloads_04657 ON events_04657.Id = payloads_04657.Id
ORDER BY events_04657.Time LIMIT 5;

SELECT 'with a local parallel replicas plan';
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;

SELECT events_04657.Time, events_04657.Id, coalesce(nullIf(payloads_04657.Payload, ''), 'NULL') AS Payload
FROM events_04657 LEFT JOIN payloads_04657 ON events_04657.Id = payloads_04657.Id
ORDER BY events_04657.Time LIMIT 5;

SELECT events_04657.Time, events_04657.Id, coalesce(nullIf(payloads_04657.Payload, ''), 'NULL') AS Payload
FROM events_04657 LEFT JOIN payloads_04657 ON events_04657.Id = payloads_04657.Id
ORDER BY events_04657.Time DESC LIMIT 5;

DROP TABLE payloads_04657;
DROP TABLE events_04657;
