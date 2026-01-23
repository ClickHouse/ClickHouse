SET enable_parallel_replicas=1,
    max_parallel_replicas=3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'parallel_replicas';

SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS events;
CREATE TABLE events ( `Id` UInt64, `Payload` String, `Time` DateTime ) ENGINE = Memory;
INSERT INTO events SELECT number, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(10);

DROP TABLE IF EXISTS attributes;
CREATE TABLE attributes ( `EventId` UInt64, `Attribute` String ) ENGINE = MergeTree ORDER BY EventId;
INSERT INTO attributes SELECT 32 AS EventId, 'attr' AS Attribute;

SELECT count(), countIf(t1.Attribute != ''), sum(sipHash64(t1.Attribute))
FROM events AS t0 INNER JOIN attributes AS t1 ON t1.EventId = t0.Id;

DROP TABLE events;
DROP TABLE attributes;
