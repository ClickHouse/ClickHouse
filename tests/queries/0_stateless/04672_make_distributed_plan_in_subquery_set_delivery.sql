-- Tags: no-old-analyzer
-- The direct set path under `make_distributed_plan`: the set is built once on the initiator,
-- either during planning when index analysis requests it or by the initiator pipeline at query
-- start, and its values ship with the worker tasks.

CREATE TABLE t_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_small (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_lc (s LowCardinality(String), k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_big SELECT number, number FROM numbers(1000);
INSERT INTO t_small SELECT number, number * 2 FROM numbers(100);
INSERT INTO t_lc SELECT toString(number % 4), number FROM numbers(1000);

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

SELECT '-- a non-key filter set ships too';
SELECT count() FROM t_big WHERE v IN (SELECT val FROM t_small WHERE id < 50);

SELECT '-- a set outside storage filters is built by the initiator pipeline at query start';
SELECT sum(k IN (SELECT val FROM t_small WHERE id < 50)) FROM t_big;

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

-- The set has exactly 100 distinct values, so the boundary is strict in both overflow modes.
SELECT '-- a set of exactly max_rows_to_transfer rows ships';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_rows_to_transfer = 100;
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_rows_to_transfer = 100, transfer_overflow_mode = 'break';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_rows_to_transfer = 99; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- 100 UInt64 values = 800 bytes: a limit above that must pass even though the dedup hash table
-- allocates more, and a limit below it must throw.
SELECT '-- the byte limit measures the shipped values, not the build memory';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_bytes_to_transfer = 1000;
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small) SETTINGS max_bytes_to_transfer = 100; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- Analysis types `in` over a `LowCardinality` argument as plain `UInt8`; the function rebuild
-- on the receiving side would wrap the type, so the deserializer keeps the serialized one.
-- Without that, every such query fails on task deserialization.
SELECT '-- LowCardinality IN subquery';
SELECT count() FROM t_lc WHERE s IN (SELECT toString(id) FROM t_small WHERE id < 2);
SELECT sum(s IN (SELECT toString(id) FROM t_small WHERE id < 2)) FROM t_lc;

SELECT '-- a set source that cannot be serialized builds locally on the initiator';
SELECT count() FROM t_big WHERE k IN (SELECT number FROM numbers(200) GROUP BY number);
SELECT count() FROM t_big WHERE k IN (SELECT number FROM numbers(50) ORDER BY number DESC LIMIT 10);

SELECT '-- two sets in one query';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50) AND v IN (SELECT val FROM t_small WHERE id < 25);

SELECT '-- IN over a tuple of columns';
SELECT count() FROM t_big WHERE (k, v) IN (SELECT id, val FROM t_small WHERE id < 50);

SELECT '-- IN in HAVING (the set is consumed above the aggregation)';
SELECT k % 7 AS g, count() FROM t_big GROUP BY g HAVING g IN (SELECT id FROM t_small WHERE id < 3) ORDER BY g;

SELECT '-- NULL in the set follows transform_null_in';
SELECT count() FROM t_big WHERE if(k = 999, NULL, k) IN (SELECT if(id = 0, NULL, val) FROM t_small WHERE id < 50);
SELECT count() FROM t_big WHERE if(k = 999, NULL, k) IN (SELECT if(id = 0, NULL, val) FROM t_small WHERE id < 50) SETTINGS transform_null_in = 1;

-- A plan over a non-serializable read collapses to a single local stage; the detached sets are
-- added back to the collapsed plan and expand the ordinary local way.
SELECT '-- value-producing IN in a plan that collapses to a single local stage';
SELECT (number IN (SELECT val FROM t_small WHERE id < 50)) AS flag FROM numbers(6);

DROP TABLE t_lc;
DROP TABLE t_big;
DROP TABLE t_small;
