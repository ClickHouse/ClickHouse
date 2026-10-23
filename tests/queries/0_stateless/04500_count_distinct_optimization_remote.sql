-- https://github.com/ClickHouse/ClickHouse/pull/81944
--
-- `count_distinct_optimization` rewrites `countDistinct(x)` /
-- `uniqExact(x)` into `count() FROM (SELECT x GROUP BY x)`. This rewrite is wrong for
-- distributed reads with `distributed_group_by_no_merge = 1`: the inner `GROUP BY` is
-- completed independently on each shard and is not merged on the initiator, so the outer
-- `count()` would count duplicate per-shard groups instead of the global distinct keys.
-- The rewrite must therefore be skipped when the source is a remote table reached through
-- the `remote(...)` table function (a `TableFunctionNode`), so that
-- `count_distinct_optimization = 1` keeps matching `count_distinct_optimization = 0`.
-- The companion test `04259_count_distinct_optimization_nullable` covers the local-table
-- analyzer path; `04256_merge_distributed_group_by_no_merge_nested` covers the nested
-- `StorageMerge` / `Distributed` wrapper path.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_cd_remote;
CREATE TABLE t_cd_remote (x UInt64) ENGINE = MergeTree ORDER BY tuple();
-- Two distinct non-NULL keys plus a duplicate: the distinct count is 3 per shard.
INSERT INTO t_cd_remote VALUES (1)(2)(3)(3);

-- `remote(...)` with `distributed_group_by_no_merge = 1`: each of the two shards aggregates
-- independently and the initiator only concatenates, so both queries must return two rows of 3.
-- Without the guard, `count_distinct_optimization = 1` would return a single wrong value (6),
-- counting the per-shard `GROUP BY` groups twice.
SELECT countDistinct(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
    SETTINGS count_distinct_optimization = 0, distributed_group_by_no_merge = 1;
SELECT countDistinct(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
    SETTINGS count_distinct_optimization = 1, distributed_group_by_no_merge = 1;
SELECT uniqExact(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
    SETTINGS count_distinct_optimization = 0, distributed_group_by_no_merge = 1;
SELECT uniqExact(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
    SETTINGS count_distinct_optimization = 1, distributed_group_by_no_merge = 1;

-- The top-level no-merge guard is also needed for an indirect remote source. Here the outer
-- query sees a `QueryNode`, rather than the `TableFunctionNode` that `isRemote` identifies.
-- Without the guard it rewrites the outer aggregate and counts the same shard-local groups
-- twice. Keep the optimization on and off results equal for both aggregate spellings.
SELECT countDistinct(x) FROM
(
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
)
SETTINGS count_distinct_optimization = 0, distributed_group_by_no_merge = 1;
SELECT countDistinct(x) FROM
(
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
)
SETTINGS count_distinct_optimization = 1, distributed_group_by_no_merge = 1;
SELECT uniqExact(x) FROM
(
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
)
SETTINGS count_distinct_optimization = 0, distributed_group_by_no_merge = 1;
SELECT uniqExact(x) FROM
(
    SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
)
SETTINGS count_distinct_optimization = 1, distributed_group_by_no_merge = 1;

-- With the default `distributed_group_by_no_merge = 0` the shard results are merged on the
-- initiator to the global distinct count (a single row of 3), and must be identical whether
-- the optimization is on or off.
SELECT countDistinct(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
    SETTINGS count_distinct_optimization = 0;
SELECT countDistinct(x) FROM remote('127.0.0.{1,2}', currentDatabase(), t_cd_remote)
    SETTINGS count_distinct_optimization = 1;

-- `StorageDistributed` also skips the coordinator merge for a `GROUP BY` on its sharding
-- key when both of these settings are enabled. The outer query sees only a `QueryNode`, so
-- the direct `isRemote()` check cannot detect this carrier. Keep the rewrite disabled and
-- ensure the result remains the global count rather than two shard-local counts.
DROP TABLE IF EXISTS t_cd_sharding;
DROP TABLE IF EXISTS t_cd_sharding_dist;
CREATE TABLE t_cd_sharding (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cd_sharding VALUES (1)(2)(3)(3);
CREATE TABLE t_cd_sharding_dist AS t_cd_sharding
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_cd_sharding, x);

SELECT countDistinct(x) FROM
(
    SELECT x FROM t_cd_sharding_dist
)
SETTINGS count_distinct_optimization = 0, optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1;
SELECT countDistinct(x) FROM
(
    SELECT x FROM t_cd_sharding_dist
)
SETTINGS count_distinct_optimization = 1, optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1;

DROP TABLE t_cd_sharding_dist;
DROP TABLE t_cd_sharding;

DROP TABLE t_cd_remote;
