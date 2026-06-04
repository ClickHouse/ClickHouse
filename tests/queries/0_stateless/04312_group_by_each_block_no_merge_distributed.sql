-- Regression tests for the `group_by_each_block_no_merge` streaming GROUP BY setting.

SET group_by_each_block_no_merge = 1;
SET max_block_size = 1000;

-- Finite input: the result is produced per block and is intentionally not merged across blocks,
-- but no row must be lost (in particular the last block must be flushed). The partial group
-- counts still sum up to the total number of rows, and the partial sums to the total sum.
SELECT sum(c), sum(s) FROM (SELECT number DIV 113 AS k, count() AS c, sum(number) AS s FROM numbers(10000) GROUP BY k);

-- Because groups can be split across block boundaries, the number of produced (unmerged) rows
-- is at least the number of distinct keys.
SELECT count() >= (SELECT uniqExact(number DIV 113) FROM numbers(10000)) FROM (SELECT number DIV 113 AS k, count() AS c FROM numbers(10000) GROUP BY k);

-- Distributed (two-stage) aggregation used to throw `Bad cast .. to ColumnAggregateFunction`
-- because the first stage produced finalized columns instead of intermediate states.
-- Now the first stage honors `final` and emits states, which are merged on the initiator,
-- so the distributed result is fully merged and correct.
SELECT sum(c), sum(s) FROM (SELECT number DIV 113 AS k, count() AS c, sum(number) AS s FROM cluster('test_cluster_two_shards', numbers(10000)) GROUP BY k);
SELECT * FROM (SELECT number DIV 113 AS k, count() AS c, sum(number) AS s FROM cluster('test_cluster_two_shards', numbers(10000)) GROUP BY ALL LIMIT 100) ORDER BY k FORMAT Null;
