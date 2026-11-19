-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert, no-random-merge-tree-settings
-- no-replicated-database: the fixture needs exactly two replicas of one table
-- no-shared-merge-tree: SharedMergeTree has no insert quorum
-- no-async-insert: a quorum insert needs insert_quorum_parallel
-- no-random-merge-tree-settings: every part must be wide, compact parts publish no per-column sizes

-- A read pinned to a block-number boundary (select_sequential_consistency clamps the read to the parts
-- confirmed by the insert quorum) must estimate PREWHERE column sizes from the same pinned part set:
-- an unconfirmed part that execution never reads must not influence which conditions are moved.

SET enable_analyzer = 1, explain_query_plan_default = 'legacy';
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 0;
-- Keep the conditions on the columns themselves: rewriting them to `.size` subcolumns changes what is measured.
-- The conditions are spelled as `notEmpty` directly, so the randomized `optimize_empty_string_comparisons`
-- rewrite of `!= ''` cannot change the printed plan.
SET optimize_functions_to_subcolumns = 0;
SET query_plan_optimize_primary_key = 1, convert_query_to_cnf = 0, enable_parallel_replicas = 0;
SET use_statistics = 0, materialize_statistics_on_insert = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET insert_keeper_fault_injection_probability = 0;

DROP TABLE IF EXISTS t_prewhere_pinned_r1 SYNC;
DROP TABLE IF EXISTS t_prewhere_pinned_r2 SYNC;

CREATE TABLE t_prewhere_pinned_r1 (k UInt64, a String CODEC(NONE), b String CODEC(NONE), payload String CODEC(NONE))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05211/t', 'r1') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_prewhere_pinned_r2 (k UInt64, a String CODEC(NONE), b String CODEC(NONE), payload String CODEC(NONE))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05211/t', 'r2') ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_prewhere_pinned_r1;
SYSTEM STOP MERGES t_prewhere_pinned_r2;

-- The confirmed part: `a` and `b` are tiny next to `payload`, so both conditions are cheap enough to move (`a` first, it is the smaller one).
INSERT INTO t_prewhere_pinned_r1 SELECT number, repeat('a', 4), repeat('b', 8), repeat('p', 1024) FROM numbers(2000);
SYSTEM SYNC REPLICA t_prewhere_pinned_r2;

SYSTEM STOP FETCHES t_prewhere_pinned_r2;

-- With fetches stopped on r2 the quorum is unsatisfiable: the insert fails, the part lands locally on r1
-- and `/quorum/status` stays set, so select_sequential_consistency = 1 pins the read below this part.
-- In this part `b` is huge: counted table-wide it dominates the queried bytes and `b` stays in WHERE.
INSERT INTO t_prewhere_pinned_r1 SELECT number + 1000000, repeat('a', 4), repeat('b', 8192), repeat('p', 8) FROM numbers(2000)
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 0, max_insert_threads = 1; -- { serverError UNKNOWN_STATUS_OF_INSERT,UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE,TIMEOUT_EXCEEDED }

-- Preconditions: two wide parts, a pending quorum, and a pinned read that sees only the first part.
SELECT 'parts', count(), countIf(part_type = 'Wide') FROM system.parts
WHERE database = currentDatabase() AND table = 't_prewhere_pinned_r1' AND active;

SELECT 'quorum_status', count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/test_05211/t/quorum' AND name = 'status';

SELECT 'unpinned_rows', count() FROM t_prewhere_pinned_r1 SETTINGS select_sequential_consistency = 0, optimize_trivial_count_query = 0;
SELECT 'pinned_rows', count() FROM t_prewhere_pinned_r1 SETTINGS select_sequential_consistency = 1, optimize_trivial_count_query = 0;

-- Unpinned: every part counts, `b` is the heaviest queried column and only `a` moves to PREWHERE.
SET select_sequential_consistency = 0;
SELECT replaceRegexpAll(explain, '__table1\.|_String', '')
FROM (EXPLAIN actions = 1 SELECT sum(length(payload)) FROM t_prewhere_pinned_r1 WHERE notEmpty(a) AND notEmpty(b))
WHERE explain LIKE '%Prewhere filter column%';

-- Pinned: the unconfirmed part is excluded from the estimate exactly as from the read, so `b` is cheap
-- again and both conditions move to PREWHERE.
SET select_sequential_consistency = 1;
SELECT replaceRegexpAll(explain, '__table1\.|_String', '')
FROM (EXPLAIN actions = 1 SELECT sum(length(payload)) FROM t_prewhere_pinned_r1 WHERE notEmpty(a) AND notEmpty(b))
WHERE explain LIKE '%Prewhere filter column%';

SELECT 'pinned_result', sum(length(payload)) FROM t_prewhere_pinned_r1 WHERE notEmpty(a) AND notEmpty(b);

SYSTEM START FETCHES t_prewhere_pinned_r2;
DROP TABLE t_prewhere_pinned_r1 SYNC;
DROP TABLE t_prewhere_pinned_r2 SYNC;
