-- Some `partitioned_hash` builds exceed `max_bytes_before_external_join` only because of the temporary
-- memory the scatter needs after the build blocks arrive, not because of the hash table. For those the join
-- scatters the build blocks in several contiguous ranges, one range at a time, and stays in memory instead
-- of switching to `GraceHashJoin`. With a lower budget the same build does switch. Both paths must return
-- what `hash` returns.
--
-- `max_bytes_before_external_join` is a spill trigger, not a memory cap (`max_memory_usage` is), so the
-- test asserts only what splitting the scatter guarantees: no switch to grace, more than one scatter range,
-- and the same result.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET max_block_size = 4096;
SET max_threads = 8;

-- Three budgets between 82 and 86 million bytes. The predicted memory of this build is about 81 million
-- bytes with the split and about 96 million without it, so a two-million-byte difference in the byte
-- accounting between architectures still leaves at least one budget inside the range.
SELECT 'budget 82 million bytes', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 82000000) AS pa)
SETTINGS log_comment = '05044 grouped';

SELECT 'budget 84 million bytes', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 84000000) AS pa)
SETTINGS log_comment = '05044 grouped';

SELECT 'budget 86 million bytes', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 86000000) AS pa)
SETTINGS log_comment = '05044 grouped';

SELECT 'budget 50 million bytes: the same build switches to grace', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0) AS h,
    (SELECT (count(), sum(p.v)) FROM (SELECT number AS k, number + 1 AS k2, number AS v FROM numbers(300000)) AS p INNER JOIN (SELECT number % 300000 AS k, (number % 300000) + 1 AS k2 FROM numbers(1500000)) AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash', max_bytes_before_external_join = 50000000) AS pa)
SETTINGS log_comment = '05044 spilled';

SYSTEM FLUSH LOGS query_log;

-- The distinct-key estimate that decides the split comes from one HyperLogLog sketch per build thread,
-- and how many build threads ran varies under load. That can move the decision for a budget at the edge
-- of the range, so the test requires only one of the three budgets to split and stay in memory.
SELECT '-- at least one build split its scatter and stayed in memory';
SELECT countIf(ProfileEvents['PartitionedHashJoinScatterGroups'] > 1
               AND ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] = 0) > 0 AS split_in_memory,
       min(ProfileEvents['PartitionedHashJoinPartitions']) > 1 AS partitioned
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05044 grouped';

SELECT '-- the build with the lower budget did switch';
SELECT max(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin']) > 0 AS spilled
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05044 spilled';
