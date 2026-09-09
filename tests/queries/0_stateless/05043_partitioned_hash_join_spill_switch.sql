-- `join_algorithm = 'partitioned_hash'` under an automatic external-join threshold is wrapped in
-- `SpillingHashJoin`. A build that exceeds `max_bytes_before_external_join` drains into
-- `GraceHashJoin` and still matches `hash`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;

SELECT '-- plan keeps partitioned_hash inside the spilling wrapper';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT number AS k FROM numbers(10)) AS t1
    INNER JOIN (SELECT number AS k FROM numbers(10)) AS t2 ON t1.k = t2.k
    SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 1
) WHERE explain LIKE '%Algorithm: SpillingHashJoin(PartitionedHashJoin)%';

SELECT '-- checksum vs hash';
SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(20000)) AS t1
INNER JOIN (SELECT number AS k, number AS v FROM numbers(20000)) AS t2 ON t1.k = t2.k
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;

SELECT count(), sum(t2.v)
FROM (SELECT number AS k FROM numbers(20000)) AS t1
INNER JOIN (SELECT number AS k, number AS v FROM numbers(20000)) AS t2 ON t1.k = t2.k
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 100000, log_comment = '05043_phj_spill_phj';

SYSTEM FLUSH LOGS query_log;

SELECT '-- switched to grace';
SELECT ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '05043_phj_spill_phj'
ORDER BY event_time_microseconds DESC
LIMIT 1;
