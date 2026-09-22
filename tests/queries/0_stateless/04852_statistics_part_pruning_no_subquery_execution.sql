SET explain_query_plan_default = 'legacy';
SET allow_experimental_statistics = 1;
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;

-- The assertions match plan text, so pin the settings that reshape it. Both projection settings must
-- stay 1 (the effective value is their conjunction): at 0 the first query prunes nothing even
-- without the fix.
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_stats_prune_in;

-- `c` carries min/max statistics (`basic` on a numeric type) and is NOT in the primary key, so
-- statistics part pruning is the only component whose key condition maps it to a key column.
CREATE TABLE t_stats_prune_in (a String, c UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic', index_granularity = 1;

-- A merge of the three level-0 parts would change the pinned part counts.
SYSTEM STOP MERGES t_stats_prune_in;

INSERT INTO t_stats_prune_in VALUES ('a', 1);
INSERT INTO t_stats_prune_in VALUES ('a', 100);
INSERT INTO t_stats_prune_in VALUES ('a', 200);

-- Statistics pruning declines an `IN` atom whose set is not built yet, so it prunes nothing and
-- all 3 parts are read. This pins the pruning outcome, not the absence of the subquery execution.
SELECT count() FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_stats_prune_in WHERE c IN (SELECT 1)
    SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1
) WHERE explain LIKE '%Statistics%';

-- The `globalIn` family reaches the same guard, and the reported failure used `globalNullIn`.
SELECT count() FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_stats_prune_in WHERE c GLOBAL IN (SELECT 1)
    SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1
) WHERE explain LIKE '%Statistics%';

SELECT count() FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_stats_prune_in WHERE globalNullIn(c, (SELECT 1))
    SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1
) WHERE explain LIKE '%Statistics%';

-- A literal `IN` list needs no subquery, so pruning still applies: 3 parts -> 1.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1
    SELECT count() FROM t_stats_prune_in WHERE c IN (1, 2)
    SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1
) WHERE explain LIKE '%Statistics%' OR explain LIKE '%Parts: 1/3%';

DROP TABLE t_stats_prune_in;

DROP TABLE IF EXISTS t_stats_prune_in_pk;

-- Same query shape, but now the `IN` column IS the primary key. Primary-key analysis runs before
-- pruning and builds the subquery set, so pruning finds it ready and must still prune: 3 parts -> 1.
CREATE TABLE t_stats_prune_in_pk (a String, c UInt64)
ENGINE = MergeTree ORDER BY c
SETTINGS auto_statistics_types = 'basic', index_granularity = 1;

SYSTEM STOP MERGES t_stats_prune_in_pk;

INSERT INTO t_stats_prune_in_pk VALUES ('a', 1);
INSERT INTO t_stats_prune_in_pk VALUES ('a', 100);
INSERT INTO t_stats_prune_in_pk VALUES ('a', 200);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1
    SELECT sum(c) FROM t_stats_prune_in_pk WHERE c IN (SELECT 1)
    SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1
) WHERE explain LIKE '%Statistics%' OR explain LIKE '%Parts: 1/3%';

DROP TABLE t_stats_prune_in_pk;

DROP TABLE IF EXISTS t_stats_prune_in_throwing;

-- A nested `IN` makes the subquery's source plan non-clonable, so building its set consumes the
-- plan. When the subquery then throws, the query must report the subquery's own error rather than
-- `Not-ready Set is passed as the second argument`.
CREATE TABLE t_stats_prune_in_throwing (a String, c UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic', index_granularity = 1;

INSERT INTO t_stats_prune_in_throwing VALUES ('a', 1), ('a', 100), ('a', 200);

-- Routing the outer query through parallel replicas reshapes it so the subquery is no longer the
-- consumed set source, so `enable_parallel_replicas` is pinned off rather than randomized.
SELECT count() FROM t_stats_prune_in_throwing
WHERE c IN (SELECT c FROM t_stats_prune_in_throwing WHERE throwIf(a = 'a') AND c IN (SELECT 1))
SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 1,
         enable_parallel_replicas = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Same query with pruning off, so the error above is attributable to statistics part pruning.
SELECT count() FROM t_stats_prune_in_throwing
WHERE c IN (SELECT c FROM t_stats_prune_in_throwing WHERE throwIf(a = 'a') AND c IN (SELECT 1))
SETTINGS use_skip_indexes = 0, use_statistics = 0, use_statistics_for_part_pruning = 0,
         enable_parallel_replicas = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE t_stats_prune_in_throwing;
