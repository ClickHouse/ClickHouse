-- The dynamic TopN threshold filter shares PREWHERE with the read's other filters
-- instead of replacing them, and it occupies the first conjunct.

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

DROP TABLE t_topk_prewhere;

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
DROP TABLE t_topk_proj;
