-- Tags: no-parallel
-- ^ test_cluster_two_shards_different_databases uses fixed shard_0 / shard_1 databases.
-- Every query pins optimize_skip_unused_shards and optimize_distributed_group_by_sharding_key, and the
-- EXPLAIN assertions only check for MergingAggregated, so the test is deterministic under randomization.

-- optimize_skip_unused_shards must not change column name resolution. In a global distributed join
-- where both sides have a column of the same name, the shard-processing-stage optimization used to
-- build a type-checked expression DAG over unqualified names, collapsing l.c and r.c onto one node and
-- throwing ILLEGAL_TYPE_OF_ARGUMENT. See https://github.com/ClickHouse/ClickHouse/issues/111089

-- The guarded optimization lives in getOptimizedQueryProcessingStageAnalyzer, which only runs under the
-- analyzer. Pin it so the plan-shape (MergingAggregated) assertions are deterministic in the old-analyzer job.
SET enable_analyzer = 1;

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.nb_l (k UInt32, a UInt16, c String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.nb_l (k UInt32, a UInt16, c String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_0.nb_r (a UInt32, c Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE shard_1.nb_r (a UInt32, c Int64) ENGINE = MergeTree ORDER BY a;

CREATE TABLE nb_dl (k UInt32, a UInt16, c String)
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', nb_l, k);
CREATE TABLE nb_dr (a UInt32, c Int64)
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', nb_r, a);

INSERT INTO nb_dl SELECT number, number % 50, toString(number) FROM numbers(20) SETTINGS distributed_foreground_insert = 1;
INSERT INTO nb_dr SELECT number % 50, toInt64(number) FROM numbers(40) SETTINGS distributed_foreground_insert = 1;

-- The following queries threw Code 43 ILLEGAL_TYPE_OF_ARGUMENT with optimize_skip_unused_shards = 1.
-- They must now return the same rows as with optimize_skip_unused_shards = 0.

-- DISTINCT with the bare column before the expression over the same-named column of the other side.
SELECT DISTINCT l.c, r.c + 2 FROM nb_dl AS l INNER JOIN nb_dr AS r ON l.a = r.a
ORDER BY 1, 2
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- LEFT JOIN variant.
SELECT DISTINCT l.c, r.c + 2 FROM nb_dl AS l LEFT JOIN nb_dr AS r ON l.a = r.a
ORDER BY 1, 2
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- GROUP BY (grouping-key path, not DISTINCT-specific).
SELECT l.c, r.c + 2 FROM nb_dl AS l INNER JOIN nb_dr AS r ON l.a = r.a
GROUP BY l.c, r.c + 2
ORDER BY 1, 2
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- CROSS/comma join variant.
SELECT DISTINCT l.c, r.c + 2 FROM nb_dl AS l, nb_dr AS r WHERE l.a = r.a
ORDER BY 1, 2
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- LIMIT BY path (also served by the guarded isShardingKeySuitsQueryTreeNodeExpression). Same
-- same-named / incompatible-type collision must return the same rows as with the optimization off.
SELECT l.c, r.c + 2 FROM nb_dl AS l INNER JOIN nb_dr AS r ON l.a = r.a
ORDER BY l.c, r.c + 2
LIMIT 1 BY l.c, r.c + 2
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- A join without a same-name collision (l.k vs r.c) is not affected by the guard: it must still
-- return the correct rows with optimize_skip_unused_shards = 1.
SELECT DISTINCT l.k, r.c + 2 FROM nb_dl AS l INNER JOIN nb_dr AS r ON l.a = r.a
ORDER BY 1, 2
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- A CROSS / comma join never null-extends either side, so grouping by this table's own sharding key
-- (`l.k`) alongside another side's column keeps every group on one shard: the aggregation must push to
-- the shard (no MergingAggregated on the initiator). The join-tree keeps CROSS joins under a
-- CrossJoinNode; treating it as null-extending would wrongly disable the optimization here.
SELECT count() = 0 FROM (
    EXPLAIN distributed = 1
    SELECT l.k, r.c FROM nb_dl AS l CROSS JOIN nb_dr AS r
    GROUP BY l.k, r.c
    SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1
) WHERE explain ILIKE '%MergingAggregated%';

SELECT '---';

-- The shard-local CROSS-join result must match the non-optimized one.
SELECT groupArray((lk, rc, cnt)) = (
    SELECT groupArray((lk, rc, cnt)) FROM (
        SELECT l.k AS lk, r.c AS rc, count() AS cnt FROM nb_dl AS l CROSS JOIN nb_dr AS r
        GROUP BY l.k, r.c ORDER BY l.k, r.c SETTINGS optimize_skip_unused_shards = 0))
FROM (
    SELECT l.k AS lk, r.c AS rc, count() AS cnt FROM nb_dl AS l CROSS JOIN nb_dr AS r
    GROUP BY l.k, r.c ORDER BY l.k, r.c SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1)
SETTINGS distributed_product_mode = 'global';

SELECT '---';

-- ARRAY JOIN (kept under an ArrayJoinNode in the join tree) only replicates the input rows; the base
-- table's sharding key (`k`) keeps its value on every produced row, so grouping by it stays shard-local.
-- LEFT ARRAY JOIN additionally emits a default row for an empty array but still preserves `k`. Both must
-- take the shard-local path (no MergingAggregated) and return the same rows as with the optimization off.
CREATE TABLE shard_0.aj (k UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.aj (k UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE daj (k UInt32, arr Array(UInt32)) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', aj, k);
INSERT INTO daj SELECT number, if(number % 2 = 0, [number, number + 100], []) FROM numbers(20) SETTINGS distributed_foreground_insert = 1;

SELECT count() = 0 FROM (
    EXPLAIN distributed = 1
    SELECT k, x FROM daj ARRAY JOIN arr AS x
    GROUP BY k, x
    SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1
) WHERE explain ILIKE '%MergingAggregated%';

SELECT '---';

SELECT count() = 0 FROM (
    EXPLAIN distributed = 1
    SELECT k, x FROM daj LEFT ARRAY JOIN arr AS x
    GROUP BY k, x
    SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1
) WHERE explain ILIKE '%MergingAggregated%';

SELECT '---';

SELECT groupArray((kk, xx, cnt)) = (
    SELECT groupArray((kk, xx, cnt)) FROM (
        SELECT k AS kk, x AS xx, count() AS cnt FROM daj LEFT ARRAY JOIN arr AS x
        GROUP BY k, x ORDER BY k, x SETTINGS optimize_skip_unused_shards = 0))
FROM (
    SELECT k AS kk, x AS xx, count() AS cnt FROM daj LEFT ARRAY JOIN arr AS x
    GROUP BY k, x ORDER BY k, x SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1);

DROP TABLE daj;

SELECT '---';

-- Grouping by a right-side column whose name equals the left sharding key (`k`) must not be treated as
-- sharding-key aggregation: a group can span shards, so the per-shard partials must be merged on the
-- initiator. Here every right row has k = 7 and joins with left rows on both shards, so the correct
-- result is a single group; a shard-local Complete stage would return two unmerged partials.
CREATE TABLE shard_0.sl (k UInt32, a UInt16) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.sl (k UInt32, a UInt16) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_0.sr (a UInt32, k Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE shard_1.sr (a UInt32, k Int64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dsl (k UInt32, a UInt16) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', sl, k);
CREATE TABLE dsr (a UInt32, k Int64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', sr, a);
INSERT INTO dsl SELECT number, number FROM numbers(10) SETTINGS distributed_foreground_insert = 1;
INSERT INTO dsr SELECT number, 7 FROM numbers(10) SETTINGS distributed_foreground_insert = 1;

SELECT r.k, count() FROM dsl AS l INNER JOIN dsr AS r ON l.a = r.a
GROUP BY r.k
ORDER BY r.k
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- The guard must not disable the optimization for a sound join key: grouping by this table's own
-- sharding key (`k` of nb_dl) keeps every group on one shard even alongside another side's column, so
-- shards still process the aggregation to Complete. Assert the plan pushes the aggregation to the
-- shard (no MergingAggregated on the initiator) rather than merging there.
SELECT count() = 0 FROM (
    EXPLAIN distributed = 1
    SELECT l.k, r.c FROM nb_dl AS l INNER JOIN nb_dr AS r ON l.a = r.a
    GROUP BY l.k, r.c
    SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1
) WHERE explain ILIKE '%MergingAggregated%';

SELECT '---';

-- A RIGHT / FULL JOIN can null-extend the sharded (left) table: an unmatched right row gets a NULL
-- sharding key and, because the right side is broadcast to every shard, is produced on all of them, so
-- a group keyed by it spans shards. Here dsr rows with a = 100, 101 match no dsl row, so a shard-local
-- Complete stage would return unmerged per-shard partials. The shortcut must NOT fire even though l.k is
-- the left table's sharding key. Assert the optimized result equals the non-optimized one.
INSERT INTO dsr SETTINGS distributed_foreground_insert = 1 VALUES (100, 7), (101, 7);

SELECT groupArray((lk, rk, c)) = (
    SELECT groupArray((lk, rk, c)) FROM (
        SELECT l.k AS lk, r.k AS rk, count() AS c FROM dsl AS l RIGHT JOIN dsr AS r ON l.a = r.a
        GROUP BY l.k, r.k ORDER BY l.k, r.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS lk, r.k AS rk, count() AS c FROM dsl AS l RIGHT JOIN dsr AS r ON l.a = r.a
    GROUP BY l.k, r.k ORDER BY l.k, r.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

SELECT '---';

-- The RIGHT JOIN plan must merge on the initiator (MergingAggregated present) rather than take the
-- shard-local shortcut.
SELECT count() > 0 FROM (
    EXPLAIN distributed = 1
    SELECT l.k, r.k FROM dsl AS l RIGHT JOIN dsr AS r ON l.a = r.a
    GROUP BY l.k, r.k
    SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1
) WHERE explain ILIKE '%MergingAggregated%';

SELECT '---';

-- The shard-local precheck must follow the same column visibility as the type-checked DAG it guards:
-- an ALIAS column is represented by its expression, not its own name (see PlannerActionsVisitor). `k_alias`
-- on the left is `ALIAS k` (the sharding key) and reduces to `k`; the right table has a distinct physical
-- column that happens to also be named `k_alias`. The DAG never sees a `k_alias` collision, so grouping by
-- both stays shard-local (the sharding key still pins each group). Counting the wrapper name as a second
-- source would wrongly disable the optimization. Assert the aggregation is pushed to the shard.
CREATE TABLE shard_0.wl (k UInt32, a UInt16, k_alias UInt32 ALIAS k) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.wl (k UInt32, a UInt16, k_alias UInt32 ALIAS k) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_0.wr (a UInt32, k_alias UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE shard_1.wr (a UInt32, k_alias UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE dwl (k UInt32, a UInt16, k_alias UInt32 ALIAS k)
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', wl, k);
CREATE TABLE dwr (a UInt32, k_alias UInt32)
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', wr, a);
INSERT INTO dwl (k, a) SELECT number, number % 50 FROM numbers(20) SETTINGS distributed_foreground_insert = 1;
INSERT INTO dwr SELECT number % 50, number FROM numbers(40) SETTINGS distributed_foreground_insert = 1;

SELECT count() = 0 FROM (
    EXPLAIN distributed = 1
    SELECT l.k_alias, r.k_alias FROM dwl AS l INNER JOIN dwr AS r ON l.a = r.a
    GROUP BY l.k_alias, r.k_alias
    SETTINGS distributed_product_mode = 'global', optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1
) WHERE explain ILIKE '%MergingAggregated%';

SELECT '---';

-- Same for DISTINCT, and the result must match the non-optimized one.
SELECT groupArray((ka, rka)) = (
    SELECT groupArray((ka, rka)) FROM (
        SELECT DISTINCT l.k_alias AS ka, r.k_alias AS rka FROM dwl AS l INNER JOIN dwr AS r ON l.a = r.a
        ORDER BY ka, rka SETTINGS optimize_skip_unused_shards = 0))
FROM (
    SELECT DISTINCT l.k_alias AS ka, r.k_alias AS rka FROM dwl AS l INNER JOIN dwr AS r ON l.a = r.a
    ORDER BY ka, rka SETTINGS optimize_skip_unused_shards = 1)
SETTINGS distributed_product_mode = 'global';

DROP TABLE dwl;
DROP TABLE dwr;
DROP TABLE nb_dl;
DROP TABLE nb_dr;
DROP TABLE dsl;
DROP TABLE dsr;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
