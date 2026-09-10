-- When an auto-spill threshold is set, the hash join is wrapped in `SpillingHashJoin`.
-- The wrapper must still run the post-build phase of the join it actually ends up using,
-- otherwise an in-memory `HashJoin` silently loses `tryConvertToFixedHashMap`,
-- `tryRerangeRightTableData` and `publishSharedRuntimeFilters`.
--
-- The threshold is given as an absolute value (rather than via
-- `max_bytes_ratio_before_external_join`, which derives it from the server's memory
-- tracker) so the wrapper is used deterministically and never actually spills.

DROP TABLE IF EXISTS t_post_build_left;
DROP TABLE IF EXISTS t_post_build_right;

CREATE TABLE t_post_build_left (k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_post_build_right (k UInt32) ENGINE = MergeTree ORDER BY k;

-- Dense UInt32 keys under the 2^18 limit, so the right side qualifies for the
-- conversion to a fixed hash map that the post-build phase performs.
INSERT INTO t_post_build_left SELECT number FROM numbers(200000);
INSERT INTO t_post_build_right SELECT number FROM numbers(200000);

SELECT count()
FROM t_post_build_left AS l
INNER JOIN t_post_build_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash',
         enable_join_fixed_hash_table_conversion = 1,
         max_bytes_ratio_before_external_join = 0,
         max_bytes_before_external_join = 100000000000,
         log_comment = '04647_post_build_phase';

SYSTEM FLUSH LOGS query_log;

-- The post-build phase ran at all (it is skipped entirely when the wrapper does not
-- report having one), and the join stayed in memory rather than spilling.
SELECT
    ProfileEvents['JoinBuildPostProcessingMicroseconds'] > 0 AS post_build_phase_ran,
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] AS spills
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04647_post_build_phase'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_post_build_left;
DROP TABLE t_post_build_right;
