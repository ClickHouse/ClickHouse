-- Tags: distributed

-- Distributed regression: the AFTER/UNTIL boundary predicate references a column (y) that is not in
-- the SELECT list. When a shard produces a mergeable aggregation-state stage (e.g. forced here via
-- distributed_group_by_no_merge = 2 or distributed_push_down_limit), the shard must still carry the
-- boundary column in its output so the initiator can evaluate LimitRangeStep over it. Otherwise the
-- shard prunes y in its Projection and the initiator fails with NOT_FOUND_COLUMN_IN_BLOCK.
-- Single localhost shard is enough to exercise the shard/initiator split.

-- { echo }

SELECT x FROM remote('127.0.0.1', view(SELECT number AS x, number * 2 AS y FROM numbers(10))) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS distributed_group_by_no_merge = 2;
SELECT x FROM remote('127.0.0.1', view(SELECT number AS x, number * 2 AS y FROM numbers(10))) ORDER BY x LIMIT AFTER y >= 6 UNTIL y >= 14 SETTINGS distributed_group_by_no_merge = 2;
SELECT x FROM remote('127.0.0.1', view(SELECT number AS x, number * 2 AS y FROM numbers(10))) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS distributed_push_down_limit = 1, distributed_group_by_no_merge = 2;
SELECT x, x + 1 AS z FROM remote('127.0.0.1', view(SELECT number AS x, number * 2 AS y FROM numbers(10))) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS distributed_group_by_no_merge = 2;
-- Legacy interpreter (enable_analyzer = 0).
SELECT x FROM remote('127.0.0.1', view(SELECT number AS x, number * 2 AS y FROM numbers(10))) ORDER BY x LIMIT 3 AFTER y >= 10 SETTINGS distributed_group_by_no_merge = 2, enable_analyzer = 0;
SELECT x FROM remote('127.0.0.1', view(SELECT number AS x, number * 2 AS y FROM numbers(10))) ORDER BY x LIMIT AFTER y >= 6 UNTIL y >= 14 SETTINGS distributed_group_by_no_merge = 2, enable_analyzer = 0;
