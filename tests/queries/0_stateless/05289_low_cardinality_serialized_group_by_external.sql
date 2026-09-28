-- Multi-key GROUP BY on `LowCardinality(String)` keys serializes the dictionary values directly.
-- The stored keys are deserialized again by external aggregation and by the merge of partial
-- aggregation states, so compare those results with the in-memory aggregation. The empty string
-- is stored in the default slot of the dictionary (index 0).

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS lc_serialized_group_by_external;
CREATE TABLE lc_serialized_group_by_external
(
    s LowCardinality(String),
    t LowCardinality(String),
    k UInt64
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO lc_serialized_group_by_external
SELECT if(number % 13 = 0, '', 'key_' || toString(number % 101)), if(number % 17 = 0, '', toString(number % 997)), number
FROM numbers(200000);

-- The second part builds its dictionaries in a different order.
INSERT INTO lc_serialized_group_by_external
SELECT if(number % 7 = 0, '', 'key_' || toString((number * 3 + 1) % 101)), if(number % 5 = 0, '', toString((number * 7 + 3) % 997)), number
FROM numbers(200000);

SELECT 'in-memory',
    count(), sum(c), sum(sv), sum(cityHash64(s, t) * c), countIf(s = ''), countIf(t = '')
FROM
(
    SELECT s, t, count() AS c, sum(k) AS sv
    FROM lc_serialized_group_by_external
    GROUP BY s, t
)
SETTINGS max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'external',
    count(), sum(c), sum(sv), sum(cityHash64(s, t) * c), countIf(s = ''), countIf(t = '')
FROM
(
    SELECT s, t, count() AS c, sum(k) AS sv
    FROM lc_serialized_group_by_external
    GROUP BY s, t
)
SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0,
    group_by_two_level_threshold = 1, max_block_size = 4096, max_threads = 2, log_comment = '05289_external';

SELECT 'distributed',
    count(), sum(c), sum(sv), sum(cityHash64(s, t) * c), countIf(s = ''), countIf(t = '')
FROM
(
    SELECT s, t, count() AS c, sum(k) AS sv
    FROM remote('127.0.0.{1,2}', currentDatabase(), lc_serialized_group_by_external)
    GROUP BY s, t
)
SETTINGS distributed_aggregation_memory_efficient = 0, prefer_localhost_replica = 0;

SELECT 'distributed memory efficient',
    count(), sum(c), sum(sv), sum(cityHash64(s, t) * c), countIf(s = ''), countIf(t = '')
FROM
(
    SELECT s, t, count() AS c, sum(k) AS sv
    FROM remote('127.0.0.{1,2}', currentDatabase(), lc_serialized_group_by_external)
    GROUP BY s, t
)
SETTINGS distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1, prefer_localhost_replica = 0;

-- The in-memory and spilled results must agree group by group, not only in aggregate.
SELECT 'mismatched groups', count()
FROM
(
    SELECT s, t, c, sv
    FROM
    (
        SELECT s, t, count() AS c, sum(k) AS sv
        FROM lc_serialized_group_by_external
        GROUP BY s, t
        SETTINGS max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0
    )
    EXCEPT
    SELECT s, t, c, sv
    FROM
    (
        SELECT s, t, count() AS c, sum(k) AS sv
        FROM lc_serialized_group_by_external
        GROUP BY s, t
        SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0,
            group_by_two_level_threshold = 1, max_block_size = 4096, max_threads = 2
    )
);

SYSTEM FLUSH LOGS query_log;

-- Prove that the external query actually spilled.
SELECT ProfileEvents['ExternalAggregationWritePart'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '05289_external';

DROP TABLE lc_serialized_group_by_external;
