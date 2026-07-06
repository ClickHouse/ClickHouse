-- Test that check the correctness of the result for optimize_aggregation_in_order and projections,
-- not that this optimization will take place.

DROP TABLE IF EXISTS normal;

CREATE TABLE normal
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            value
        ORDER BY ts, key
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO normal SELECT
    number,
    toDateTime('2021-12-06 00:00:00') + number,
    number
FROM numbers(100000);

SET force_optimize_projection=1;
SET optimize_use_projections=1, optimize_aggregation_in_order=1, enable_parallel_replicas=0;

WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY a ORDER BY v LIMIT 5;
WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY toStartOfHour(ts), a ORDER BY v LIMIT 5;

SET optimize_aggregation_in_order=0;
SET enable_parallel_replicas=1, parallel_replicas_local_plan=1, parallel_replicas_support_projection=1, parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY a ORDER BY v LIMIT 5;
WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM normal WHERE ts > '2021-12-06 22:00:00' GROUP BY toStartOfHour(ts), a ORDER BY v LIMIT 5;

DROP TABLE normal;

DROP TABLE IF EXISTS agg;

CREATE TABLE agg
(
    `key` UInt32,
    `ts` DateTime,
    `value` UInt32,
    PROJECTION aaaa
    (
        SELECT
            ts,
            key,
            sum(value)
        GROUP BY ts, key
    )
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO agg SELECT
    1,
    toDateTime('2021-12-06 00:00:00') + number,
    number
FROM numbers(100000);

SET optimize_use_projections=1, optimize_aggregation_in_order=1, enable_parallel_replicas=0;

WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY a ORDER BY v LIMIT 5;
WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY toStartOfHour(ts), a ORDER BY v LIMIT 5;

SET optimize_aggregation_in_order=0;
SET enable_parallel_replicas=1, parallel_replicas_local_plan=1, parallel_replicas_support_projection=1, parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY a ORDER BY v LIMIT 5;
WITH toStartOfHour(ts) AS a SELECT sum(value) v FROM agg WHERE ts > '2021-12-06 22:00:00' GROUP BY toStartOfHour(ts), a ORDER BY v LIMIT 5;

DROP TABLE agg;
