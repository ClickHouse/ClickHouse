-- The dynamic TopN threshold filter shares PREWHERE with the read's other filters
-- instead of replacing them, and it occupies the first conjunct.
-- Tags: no-parallel-replicas
-- Every assertion below pins an exact read-step shape. `clickhouse-test` enables
-- `enable_parallel_replicas` for an untagged test whenever it draws
-- `automatic_parallel_replicas_mode = 2`, and the `ParallelReplicas` CI flavour runs untagged tests
-- with it on; either replaces the read step this file asserts on.

-- `legacy` prints the raw filter-column name, whose argument order is the DAG child order and
-- therefore the order the PREWHERE read steps run in. The pretty renderer walks the conjunction
-- through a stack and reports the atoms reversed, so it cannot be used to assert placement.
SET explain_query_plan_default = 'legacy';

SET query_plan_max_limit_for_top_k_optimization = 1000; -- pin to default so LIMIT 10 always qualifies
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 0; -- exercise the dynamic-filter arm, not the skip-index arm
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;
SET optimize_use_projections = 1; -- the projection section below needs the projection to be picked
SET optimize_read_in_order = 1; -- the sorted-projection section below is decided from the read order

DROP TABLE IF EXISTS t_topk_prewhere;

CREATE TABLE t_topk_prewhere (k UInt32, pred UInt32, tag String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_topk_prewhere
SELECT number, number % 10, concat('t', toString(number % 7)) FROM numbers(50000);

-- An explicit PREWHERE no longer disables dynamic filtering.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere PREWHERE pred = 3 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%__topKFilter%';

-- The user's own condition stays in the PREWHERE, and the threshold filter takes the first slot.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere PREWHERE pred = 3 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(k), equals(pred, 3%';

-- A plain WHERE is promoted into the same conjunction rather than being left above the read.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere WHERE pred = 3 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(k), equals(%';

-- Several conditions stay a flat conjunction: `MergeTreeSplitPrewhereIntoReadSteps` splits on the
-- root's direct children, so a nested `and` would collapse the multi-condition PREWHERE
-- into a single read step.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere PREWHERE pred = 3 AND tag = 't2' ORDER BY k LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(k), equals(pred, 3%tag%';

-- Same rows either way. The ORDER BY is on a unique column, so the top-K has no ties.
SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere PREWHERE pred = 3 ORDER BY k LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere PREWHERE pred = 3 ORDER BY k LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere WHERE pred = 3 AND tag = 't2' ORDER BY k LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere WHERE pred = 3 AND tag = 't2' ORDER BY k LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

-- A stateful condition keeps the read out of the conjunction: the read steps run in conjunct order
-- and each filters the block the next one sees, so such a condition would observe a different block
-- than it does as the only condition.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere PREWHERE rowNumberInBlock() < 1 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';

-- Sensitivity control for that pattern: the same comparison over a non-stateful operand installs it.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere PREWHERE pred < 1 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';

-- Same rows either way. Compared arm to arm instead of printed: which rows a `rowNumberInBlock()`
-- condition keeps depends on the block size, and the test runner randomizes it.
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere PREWHERE rowNumberInBlock() < 1 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0)
  = (SELECT groupArray(k) FROM (SELECT k FROM t_topk_prewhere PREWHERE rowNumberInBlock() < 1 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1);

-- A non-deterministic condition is kept out for the same reason even when it is not stateful:
-- `blockSize` reports the row count of the block it is evaluated on.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_prewhere PREWHERE blockSize() > 100 ORDER BY k LIMIT 10)
WHERE explain ILIKE '%FUNCTION \_\_topKFilter%';

DROP TABLE t_topk_prewhere;

-- Rows for that condition. It needs a read order unrelated to `k`, and enough rows for the threshold
-- to cut inside a block, so it gets its own table. Compared arm to arm for the reason above.
DROP TABLE IF EXISTS t_topk_blocksize;

CREATE TABLE t_topk_blocksize (k UInt32)
ENGINE = MergeTree ORDER BY intHash32(k)
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_topk_blocksize SELECT number FROM numbers(100000);

SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0)
  = (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1);

-- Such a condition is not filtered underneath when it is left above the read either: the threshold shrinks
-- the blocks the `WHERE` is handed. With an explicit PREWHERE present the `WHERE` stays there, since
-- `optimize_prewhere_after_pushdown` is off by default. The PREWHERE itself is one the filter does share.
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE k % 10 < 9 WHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0)
  = (SELECT groupArray(k) FROM (SELECT k FROM t_topk_blocksize PREWHERE k % 10 < 9 WHERE blockSize() > 100 ORDER BY k LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1);

DROP TABLE t_topk_blocksize;

-- A stored column whose name is the name the threshold filter's own column gets must not be taken
-- for it. The name only reaches the read's PREWHERE actions when the column takes part in the
-- promoted filter; a plain `SELECT` of it reaches the read's header instead, which is the section
-- after this one.

DROP TABLE IF EXISTS t_topk_collide;

CREATE TABLE t_topk_collide (k UInt32, pred UInt32, `__topKFilter(k)` UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_topk_collide SELECT number, number % 10, 0 FROM numbers(50000);

-- Fixture control: the stored column takes part in the read's own PREWHERE conjunction, so the rows
-- below are compared on a plan that really holds both names. Matching the `Prewhere filter column:`
-- line is what scopes this to the read: an `INPUT` line alone is printed by the steps above it too.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_collide WHERE pred = 3 AND `__topKFilter(k)` = 0 ORDER BY k LIMIT 5)
WHERE explain ILIKE '%Prewhere filter column: %equals(%\_\_topKFilter(k)%';

SELECT groupArray(k) FROM (
    SELECT k FROM t_topk_collide WHERE pred = 3 AND `__topKFilter(k)` = 0 ORDER BY k LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(k) FROM (
    SELECT k FROM t_topk_collide WHERE pred = 3 AND `__topKFilter(k)` = 0 ORDER BY k LIMIT 5)
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_collide;

-- Without an existing PREWHERE the filter column is named after the generated node alone, and the
-- reader resolves that name against the block the read produces, so a stored column of the same name
-- would be removed in its place. Lazy materialization reads such a column in a separate step, but it
-- runs after the filter is installed, so the refusal covers that arm too.

DROP TABLE IF EXISTS t_topk_collide_no_prewhere;
DROP TABLE IF EXISTS t_topk_no_prewhere;

CREATE TABLE t_topk_collide_no_prewhere (k UInt32, `__topKFilter(k)` UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- Identical apart from the column's name, so the control below differs from the fixture in nothing else.
CREATE TABLE t_topk_no_prewhere (k UInt32, other UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_topk_collide_no_prewhere SELECT number, number * 37 % 251 FROM numbers(50000);
INSERT INTO t_topk_no_prewhere SELECT number, number * 37 % 251 FROM numbers(50000);

-- Both columns come from the read itself, and the query has no WHERE and no PREWHERE. The second
-- element of each tuple is the stored column, so a row that lost it would not compare equal.
SELECT groupArray((k, x)) FROM (
    SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 0, use_top_k_dynamic_filtering = 0;
SELECT groupArray((k, x)) FROM (
    SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 0, use_top_k_dynamic_filtering = 1;

-- The same shape with lazy materialization on. `query_plan_max_limit_for_lazy_materialization` is
-- pinned to its default so `LIMIT 5` always qualifies for it.
SELECT groupArray((k, x)) FROM (
    SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
    use_top_k_dynamic_filtering = 0;
SELECT groupArray((k, x)) FROM (
    SELECT k, `__topKFilter(k)` AS x FROM t_topk_collide_no_prewhere ORDER BY k LIMIT 5)
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
    use_top_k_dynamic_filtering = 1;

-- Positive control: the same shape on the table that differs only in the column's name does install
-- the filter, so the rows above cannot pass by this branch never installing at all.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k, other FROM t_topk_no_prewhere ORDER BY k LIMIT 5
    SETTINGS query_plan_optimize_lazy_materialization = 0)
WHERE explain ILIKE '%Prewhere filter column: \_\_topKFilter(k)%';

DROP TABLE t_topk_collide_no_prewhere;
DROP TABLE t_topk_no_prewhere;

-- A TopK read served by a normal projection still carries the threshold filter, and returns the same
-- rows as with the feature off. `a` is uncorrelated with the table's own sort key, so only the
-- projection can prune `a < 2000`; `s` is unique, so the top-K has no ties.

DROP TABLE IF EXISTS t_topk_proj;

CREATE TABLE t_topk_proj (id UInt64, a UInt64, s UInt64,
    PROJECTION p (SELECT id, a, s ORDER BY a))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

INSERT INTO t_topk_proj SELECT number, cityHash64(number) % 200000, 199999 - number FROM numbers(200000);
OPTIMIZE TABLE t_topk_proj FINAL;

SELECT count() > 0 FROM (
    EXPLAIN projections = 1
    SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%ReadFromMergeTree (p)%';

SELECT count() > 0 FROM (
    EXPLAIN projections = 1
    SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10
    SETTINGS optimize_use_projections = 0)
WHERE explain ILIKE '%ReadFromMergeTree (p)%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%__topKFilter%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10
    SETTINGS use_top_k_dynamic_filtering = 0)
WHERE explain ILIKE '%__topKFilter%';

-- The merged conjunction has to be the PREWHERE root on the projection read as well:
-- `tryBuildPrewhereSteps` splits on a root `and` only, so any other root collapses the two
-- conditions into a single read step that reads the sort column and the predicate column together.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(s), \_projection\_filter)%';

SELECT groupArray(id) FROM (SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(id) FROM (SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

-- A projection sorted by the TopK sort column makes the read deliver rows already ordered by it.
-- The threshold filter would then reject everything past the first `n` rows, so the LIMIT never
-- cancels the read and the whole table is scanned. The base table's own sorting key starts with
-- `id`, so only the projection's order can produce that state.

DROP TABLE IF EXISTS t_topk_proj_sorted;

CREATE TABLE t_topk_proj_sorted (id UInt64, a UInt64, s UInt64,
    PROJECTION ps (SELECT id, a, s ORDER BY s))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- `s` has ties, so the ORDER BY needs a second key and the sort suffix is finished above the read.
-- That is the shape whose threshold is published while the read is still running.
INSERT INTO t_topk_proj_sorted
SELECT number, cityHash64(number) % 200000, intHash64(number) % 100000 FROM numbers(200000);
OPTIMIZE TABLE t_topk_proj_sorted FINAL;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 10)
WHERE explain ILIKE '%ReadType: InOrder%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 10)
WHERE explain ILIKE '%__topKFilter%';

-- Positive control: without the projection the same query reads the base table, which is not
-- ordered by `s`, so the shape is still eligible and the threshold filter is installed.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 10
    SETTINGS optimize_use_projections = 0)
WHERE explain ILIKE '%__topKFilter%';

-- Negative control: a projection that is not sorted by `s` does not read in order, so the
-- suppression must not reach it.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj PREWHERE a < 2000 ORDER BY s ASC, id ASC LIMIT 10)
WHERE explain ILIKE '%__topKFilter%';

SELECT groupArray((id, s)) FROM (
    SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray((id, s)) FROM (
    SELECT id, s FROM t_topk_proj_sorted PREWHERE a < 100000 ORDER BY s ASC, id ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_proj_sorted;

-- A projection that covers only some parts leaves the base-table read next to the projection read
-- under a `Union`. Both branches have to carry the threshold filter: the base-table read is the one
-- master installed on, so losing it there would read the uncovered parts unfiltered.

DROP TABLE IF EXISTS t_topk_proj_partial;

CREATE TABLE t_topk_proj_partial (id UInt64, a UInt64, s UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- Merging the two parts would give every part the projection and collapse the union.
SYSTEM STOP MERGES t_topk_proj_partial;

INSERT INTO t_topk_proj_partial SELECT number, cityHash64(number) % 200000, 199999 - number FROM numbers(100000);
ALTER TABLE t_topk_proj_partial ADD PROJECTION p (SELECT id, a, s ORDER BY a);
INSERT INTO t_topk_proj_partial SELECT number, cityHash64(number) % 200000, 199999 - number FROM numbers(100000, 100000);

-- Fixture control: `ADD PROJECTION` is not materialized, so only the second part carries `p` and the
-- plan really is a union of two reads. Without these two rows the count below holds vacuously on a
-- plan that has one read.
SELECT count() = 1 FROM (
    EXPLAIN projections = 1
    SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%ReadFromMergeTree (p)%';
SELECT count() = 1 FROM (
    EXPLAIN projections = 1
    SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE trimLeft(explain) = 'Union';

SELECT count() = 2 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(s),%';

-- Negative control: with every part covered there is no union and a single installation.
SELECT count() = 1 FROM (
    EXPLAIN actions = 1
    SELECT id, s FROM t_topk_proj WHERE a < 2000 ORDER BY s ASC LIMIT 10)
WHERE explain ILIKE '%Prewhere filter column: and(\_\_topKFilter(s),%';

SELECT groupArray(id) FROM (
    SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 0;
SELECT groupArray(id) FROM (
    SELECT id, s FROM t_topk_proj_partial WHERE a < 2000 ORDER BY s ASC LIMIT 20)
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_proj_partial;
DROP TABLE t_topk_proj;
