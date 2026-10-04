-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- Test for https://github.com/ClickHouse/ClickHouse/issues/121133

-- Randomized to 0 or 1, and the value changes the plan the EXPLAIN blocks below print.
SET parallel_replicas_local_plan = 1;
-- Statistics are a part-pruning source of their own; pinning both off keeps the Min-Max and
-- Partition steps the only pruners the EXPLAIN blocks below can attribute a Parts: count to.
SET use_statistics = 0;
SET use_statistics_for_part_pruning = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_flat;

-- `t_flat` holds the same rows behind an unprunable key, so its answer is the row-level truth
-- that every other table below must reproduce.
CREATE TABLE t_part (c0 Nullable(Int)) ENGINE = MergeTree PARTITION BY (c0) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
CREATE TABLE t_flat (c0 Nullable(Int)) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_part VALUES (NULL), (0), (1), (5), (NULL);
INSERT INTO t_flat VALUES (NULL), (0), (1), (5), (NULL);

SELECT 'the reported oracle mismatch: count(p) + count(not p) + count(p is null) = count(*)';
SELECT (SELECT count() FROM t_part WHERE assumeNotNull(c0) = 0)
     + (SELECT count() FROM t_part WHERE NOT (assumeNotNull(c0) = 0))
     + (SELECT count() FROM t_part WHERE (assumeNotNull(c0) = 0) IS NULL)
     = (SELECT count() FROM t_part) AS tlp_identity_holds;

SELECT 'predicate, row-level truth, answer over the partitioned table';
SELECT 'assumeNotNull(c0) = 0', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) = 0), (SELECT count() FROM t_part WHERE assumeNotNull(c0) = 0);
SELECT 'assumeNotNull(c0) < 3', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) < 3), (SELECT count() FROM t_part WHERE assumeNotNull(c0) < 3);
SELECT 'NOT (assumeNotNull(c0) > 0)', (SELECT count() FROM t_flat WHERE NOT (assumeNotNull(c0) > 0)), (SELECT count() FROM t_part WHERE NOT (assumeNotNull(c0) > 0));
SELECT 'assumeNotNull(c0) >= 0', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) >= 0), (SELECT count() FROM t_part WHERE assumeNotNull(c0) >= 0);
-- A set atom carries its own copy of the chain and is applied by MergeTreeSetIndex, not by KeyCondition.
SELECT 'assumeNotNull(c0) IN (0)', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) IN (0)), (SELECT count() FROM t_part WHERE assumeNotNull(c0) IN (0));

-- `use_skip_indexes = 0` sets skip_analysis on the part-level Min-Max condition only, so the
-- Partition step becomes the sole pruner and its Parts: denominator is the table's part count.
SELECT 'the partition step is the sole pruner here, and it now keeps the NULL partition';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT c0 FROM t_part WHERE assumeNotNull(c0) = 0
    SETTINGS optimize_use_implicit_projections = 0, optimize_use_projections = 0, use_skip_indexes = 0
) WHERE explain LIKE '%Condition%' OR explain LIKE '%Parts%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%';

SELECT 'ifNull and coalesce reach the same chain once the rewrite that usually hides them is off';
SELECT 'ifNull(c0, 0) = 0', (SELECT count() FROM t_flat WHERE ifNull(c0, 0) = 0 SETTINGS allow_key_condition_coalesce_rewrite = 0), (SELECT count() FROM t_part WHERE ifNull(c0, 0) = 0 SETTINGS allow_key_condition_coalesce_rewrite = 0);
SELECT 'coalesce(c0, 0) = 0', (SELECT count() FROM t_flat WHERE coalesce(c0, 0) = 0 SETTINGS allow_key_condition_coalesce_rewrite = 0), (SELECT count() FROM t_part WHERE coalesce(c0, 0) = 0 SETTINGS allow_key_condition_coalesce_rewrite = 0);

-- An inner link can also produce a real NULL: `nullIf` maps the exact point 5 to one, and the chain
-- leaves it untransformed just like an infinity bound. Such a bound compares as both inside and
-- outside every range, so the negated atom is the form that can wrongly prune it; keep it negated.
SELECT 'NOT (coalesce(nullIf(c0, 5), 0) = 1)', (SELECT count() FROM t_flat WHERE NOT (coalesce(nullIf(c0, 5), 0) = 1) SETTINGS allow_key_condition_coalesce_rewrite = 0), (SELECT count() FROM t_part WHERE NOT (coalesce(nullIf(c0, 5), 0) = 1) SETTINGS allow_key_condition_coalesce_rewrite = 0);
-- Partition 1 is still pruned, so such a bound costs its own partition and no other.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT c0 FROM t_part WHERE NOT (coalesce(nullIf(c0, 5), 0) = 1)
    SETTINGS optimize_use_implicit_projections = 0, optimize_use_projections = 0, use_skip_indexes = 0,
             allow_key_condition_coalesce_rewrite = 0
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%';

DROP TABLE IF EXISTS t_rev;
CREATE TABLE t_rev (c0 Nullable(Int)) ENGINE = MergeTree PARTITION BY (c0) ORDER BY (c0 DESC)
SETTINGS allow_nullable_key = 1;
INSERT INTO t_rev VALUES (NULL), (0), (1), (5), (NULL);
SELECT 'reverse key', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) = 0), (SELECT count() FROM t_rev WHERE assumeNotNull(c0) = 0);

DROP TABLE IF EXISTS t_lc;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_lc (c0 LowCardinality(Nullable(Int))) ENGINE = MergeTree PARTITION BY (c0) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_lc VALUES (NULL), (0), (1), (5), (NULL);
SELECT 'LowCardinality(Nullable) key', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) = 0), (SELECT count() FROM t_lc WHERE assumeNotNull(c0) = 0);

DROP TABLE IF EXISTS t_smt;
CREATE TABLE t_smt (c0 Nullable(Int)) ENGINE = SummingMergeTree PARTITION BY (c0) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_smt VALUES (NULL), (0), (1), (NULL);
SELECT 'SummingMergeTree, the query the fuzzer reported';
SELECT (SELECT count() FROM t_smt WHERE assumeNotNull(t_smt.c0) = false)
     + (SELECT count() FROM t_smt WHERE NOT (assumeNotNull(t_smt.c0) = false))
     + (SELECT count() FROM t_smt WHERE (assumeNotNull(t_smt.c0) = false) IS NULL)
     = (SELECT count() FROM t_smt) AS tlp_identity_holds;

SELECT 'control: with no NULL row the partition step is the only pruner and still prunes';
DROP TABLE IF EXISTS t_nonull;
CREATE TABLE t_nonull (c0 Nullable(Int)) ENGINE = MergeTree PARTITION BY (c0) ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_nonull VALUES (0), (1), (5);
SELECT 'assumeNotNull(c0) = 0', count() FROM t_nonull WHERE assumeNotNull(c0) = 0;
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT c0 FROM t_nonull WHERE assumeNotNull(c0) = 0
    SETTINGS optimize_use_implicit_projections = 0, optimize_use_projections = 0, use_skip_indexes = 0
) WHERE explain LIKE '%Parts%' OR explain LIKE '%Min-Max%' OR explain LIKE '%Partition%';

SELECT 'control: the primary-key path already rejected the chain and is unchanged';
DROP TABLE IF EXISTS t_pk;
CREATE TABLE t_pk (c0 Nullable(Int)) ENGINE = MergeTree ORDER BY c0 SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_pk VALUES (NULL), (0), (1), (5), (NULL);
SELECT 'assumeNotNull(c0) = 0', (SELECT count() FROM t_flat WHERE assumeNotNull(c0) = 0), (SELECT count() FROM t_pk WHERE assumeNotNull(c0) = 0);

SELECT 'control: a NULL-preserving wrapper maps the NULL row to NULL and still answers correctly';
SELECT 'c0 + 1 = 1', (SELECT count() FROM t_flat WHERE c0 + 1 = 1), (SELECT count() FROM t_part WHERE c0 + 1 = 1);
SELECT 'round(c0) = 0', (SELECT count() FROM t_flat WHERE round(c0) = 0), (SELECT count() FROM t_part WHERE round(c0) = 0);

DROP TABLE t_part;
DROP TABLE t_flat;
DROP TABLE t_rev;
DROP TABLE t_lc;
DROP TABLE t_smt;
DROP TABLE t_nonull;
DROP TABLE t_pk;
