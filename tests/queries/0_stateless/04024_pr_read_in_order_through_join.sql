-- Regression test: parallel replicas coordination mode mismatch with read_in_order_through_join.
-- The optimization can produce different results on the initiator and remote replicas
-- (due to differences in plan construction), leading to "Replica decided to read in Default
-- mode, not in WithOrder" LOGICAL_ERROR.
-- https://github.com/ClickHouse/ClickHouse/issues/94076

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS payloads;

CREATE TABLE events (Time DateTime, Id String) ENGINE = MergeTree ORDER BY Time;
INSERT INTO events SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND AS Time, toString(number) AS Id FROM numbers(10000);

CREATE TABLE payloads (Payload String, Id String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO payloads SELECT concat('Payload ', toString(number)) AS Payload, toString(number) AS Id FROM numbers(100);

SET enable_analyzer = 1;
SET query_plan_read_in_order = 1, optimize_read_in_order = 1;
SET query_plan_read_in_order_through_join = 1;
SET optimize_aggregation_in_order = 1;

SET enable_parallel_replicas = 0;

-- Without parallel replicas: read_in_order_through_join should apply (InOrder for events table)
SELECT 'Without parallel replicas:';
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN actions = 1
    SELECT events.Time, events.Id, payloads.Payload
    FROM events LEFT JOIN payloads ON events.Id = payloads.Id
    ORDER BY events.Time
    LIMIT 3
) WHERE explain LIKE '%ReadType%';

-- With parallel replicas: read_in_order_through_join must NOT apply (Default for events table)
-- to avoid coordination mode mismatch between initiator and remote replicas.
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;

SELECT 'With parallel replicas, sorting through JOIN:';
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN actions = 1
    SELECT events.Time, events.Id, payloads.Payload
    FROM events LEFT JOIN payloads ON events.Id = payloads.Id
    ORDER BY events.Time
    LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT 'With parallel replicas, aggregation through JOIN:';
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN actions = 1
    SELECT toStartOfHour(events.Time) AS t, count()
    FROM events LEFT JOIN payloads ON events.Id = payloads.Id
    GROUP BY t
    ORDER BY t
    LIMIT 3
) WHERE explain LIKE '%ReadType%';

SELECT 'With parallel replicas, distinct through JOIN:';
SELECT groupArray(trim(explain)) FROM (
    EXPLAIN actions = 1
    SELECT DISTINCT events.Time
    FROM events LEFT JOIN payloads ON events.Id = payloads.Id
    ORDER BY events.Time
    LIMIT 3
) WHERE explain LIKE '%ReadType%';

-- Also run the actual queries with failpoints to verify no coordination mode mismatch
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

SELECT events.Time, events.Id, payloads.Payload
FROM events LEFT JOIN payloads ON events.Id = payloads.Id
ORDER BY events.Time LIMIT 3
FORMAT Null;

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

SELECT toStartOfHour(events.Time) AS t, count()
FROM events LEFT JOIN payloads ON events.Id = payloads.Id
GROUP BY t ORDER BY t LIMIT 3
FORMAT Null;

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

SELECT DISTINCT events.Time
FROM events LEFT JOIN payloads ON events.Id = payloads.Id
ORDER BY events.Time LIMIT 3
FORMAT Null;

DROP TABLE events;
DROP TABLE payloads;

SELECT 'OK';
