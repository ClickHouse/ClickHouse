DROP TABLE IF EXISTS test;

CREATE TABLE test (id UInt64, date Date)
ENGINE = MergeTree
ORDER BY id
AS select *, '2023-12-25' from numbers(100);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

-- when the query plan is serialized for distributed query, parallel replicas are not enabled because
-- (with prefer_localhost_replica) because all reading steps are ReadFromTable instead of ReadFromMergeTree
SET serialize_query_plan = 0;

SELECT count(), sum(id)
FROM remote('127.0.0.1|127.0.0.2|127.0.0.3|127.0.0.4', currentDatabase(), test)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 4, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE test;
