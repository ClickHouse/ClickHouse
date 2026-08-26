-- The post-build scatter transient, not the leaf hash tables, is what pushes some
-- `partitioned_hash` builds over `max_bytes_before_external_join`. For those the post-build gate
-- splits the scatter across contiguous build-block ranges and stays in memory, instead of draining
-- into `GraceHashJoin`. Below the band the same build does spill, and both paths must agree with
-- `hash`.
--
-- `max_bytes_before_external_join` is a spill trigger, not a memory cap (`max_memory_usage` is),
-- so what is asserted here is the outcome grouping actually guarantees: no switch to grace, more
-- than one scatter range, and an unchanged result.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET max_block_size = 4096;
SET max_threads = 8;

SELECT '-- checksum vs hash';
SELECT count(), sum(p.v)
FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p
INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b
    ON p.k = b.k AND p.k2 = b.k2
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0;

-- Three budgets in the 78-82 million byte band, so two million bytes of architecture
-- drift in the byte accounting cannot make the test miss it.
SELECT count(), sum(p.v)
FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p
INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b
    ON p.k = b.k AND p.k2 = b.k2
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 78000000,
         log_comment = '05044_phj_grouped';

SELECT count(), sum(p.v)
FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p
INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b
    ON p.k = b.k AND p.k2 = b.k2
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 80000000,
         log_comment = '05044_phj_grouped';

SELECT count(), sum(p.v)
FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p
INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b
    ON p.k = b.k AND p.k2 = b.k2
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 82000000,
         log_comment = '05044_phj_grouped';

SELECT '-- below the band the same build spills to grace instead';
SELECT count(), sum(p.v)
FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p
INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b
    ON p.k = b.k AND p.k2 = b.k2
SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 50000000,
         log_comment = '05044_phj_spilled';

SYSTEM FLUSH LOGS query_log;

-- The band is four million bytes wide, and the fill-phase distinct estimate is sampled
-- from the per-lane sketches, whose count depends on how many pipeline threads actually
-- ran. Under concurrent load that can move the decision at the very edge of the band, so
-- requiring all three budgets to stay in memory would be flaky. One of them grouping
-- without switching is what proves the mechanism.
SELECT '-- at least one build grouped its scatter without switching to grace';
SELECT countIf(ProfileEvents['PartitionedHashJoinScatterGroups'] > 1
               AND ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] = 0) > 0 AS grouped_in_memory,
       min(ProfileEvents['PartitionedHashJoinPartitions']) > 1 AS partitioned
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05044_phj_grouped';

SELECT '-- and the build below the band did switch';
SELECT max(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']) AS spilled
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05044_phj_spilled';
