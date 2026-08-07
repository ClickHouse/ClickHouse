-- Tags: no-old-analyzer
-- The direct set path under `make_distributed_plan`: the set is built once on the initiator
-- when planning-time index analysis requests it, and its values ship with the worker tasks.
-- A set no planning-time consumer built cannot ship yet and fails closed at task serialization
-- (until sets are built at executor start).

CREATE TABLE t_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_small (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_big SELECT number, number FROM numbers(1000);
INSERT INTO t_small SELECT number, number * 2 FROM numbers(100);

SET enable_analyzer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET allow_experimental_correlated_subqueries = 0, rewrite_in_to_join = 0;
-- The index settings decide whether the set is built during planning (and used by the index)
-- or at query start; pin them against randomization.
SET use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

SELECT '-- results match local execution';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50);
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50) SETTINGS make_distributed_plan = 0;

SELECT '-- NOT IN';
SELECT count() FROM t_big WHERE k NOT IN (SELECT val FROM t_small WHERE id < 50);

SELECT '-- empty set';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 0);

-- A non-key filter (`v IN (...)`) also ships whenever the storage-level in-place build runs,
-- but that build is heuristic (prewhere placement), so it is not asserted here; executor-start
-- builds will make delivery unconditional.

SELECT '-- a set outside storage filters is not built at planning and is rejected at task serialization';
SELECT sum(k IN (SELECT val FROM t_small WHERE id < 50)) FROM t_big; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- the index retention cap does not limit the shipped set';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50)
    SETTINGS use_index_for_in_with_subqueries_max_values = 5;

SELECT '-- the set subquery itself executes as a distributed plan (deduplicated before the gather)';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50)
    SETTINGS distributed_plan_default_reader_bucket_count = 3, distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- the transfer limits bound the shipped set';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_rows_to_transfer = 10; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- break overflow mode also fails closed: a built set cannot be truncated';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_rows_to_transfer = 10, transfer_overflow_mode = 'break'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

DROP TABLE t_big;
DROP TABLE t_small;
