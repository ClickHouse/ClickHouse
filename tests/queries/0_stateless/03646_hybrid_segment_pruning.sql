-- Tags: no-fasttest, no-random-settings
-- Tag no-fasttest: requires remote() table function
-- Tag no-random-settings: asserts EXPLAIN output, which randomized plan settings perturb

SET allow_experimental_hybrid_table = 1;

-- The EXPLAIN-based assertions below print plan shapes. `no-random-settings` keeps the session
-- at the defaults the reference was generated with. The settings below pin the few values that
-- differ from their defaults, plus the timezone, so the assertions do not depend on the server
-- configuration either. None of these affect pruning logic.
SET prefer_localhost_replica = 1;             -- avoid ReadFromRemote vs ReadFromMergeTree flips
SET query_plan_join_swap_table = 'false';     -- pin JOIN side ordering
SET use_query_condition_cache = 0;            -- consistent EXPLAIN across runs
SET optimize_trivial_count_query = 1;
SET parallel_replicas_local_plan = 0;
SET session_timezone = 'UTC';                 -- pin DateTime formatting in SELECT * output

DROP TABLE IF EXISTS local_hot SYNC;
DROP TABLE IF EXISTS local_cold SYNC;
DROP TABLE IF EXISTS local_warm SYNC;
DROP TABLE IF EXISTS pruning_t SYNC;
DROP TABLE IF EXISTS pruning_t3 SYNC;
DROP TABLE IF EXISTS pruning_or SYNC;
DROP TABLE IF EXISTS dim SYNC;

CREATE TABLE local_hot (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
CREATE TABLE local_cold (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
INSERT INTO local_hot VALUES ('2025-10-15', 1), ('2025-11-01', 2);
INSERT INTO local_cold VALUES ('2025-08-01', 3), ('2025-06-15', 4);

CREATE TABLE pruning_t
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('127.0.0.1:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-09-01'
AS local_hot;

-- {echoOn}

-- Test 1: Baseline (no pruning) — both segments planned, Union (Hybrid) present.
-- SELECT * surfaces the actual rows from both segments so a buggy pruner that swaps or
-- drops a segment would change the values, not just the count.
SELECT * FROM pruning_t ORDER BY ts;
SELECT * FROM pruning_t WHERE value > 0 ORDER BY ts;
EXPLAIN SELECT count() FROM pruning_t WHERE value > 0;

-- Test 2: Cold (additional) segment pruned via range contradiction — only base remains.
-- The surviving rows must be the two hot rows; the cold rows must not leak through.
SELECT * FROM pruning_t WHERE ts > '2025-10-01' ORDER BY ts;
EXPLAIN SELECT count() FROM pruning_t WHERE ts > '2025-10-01';

-- Test 3: Hot (base) segment pruned — only cold remains as a local plan.
-- The surviving rows must be the two cold rows.
SELECT * FROM pruning_t WHERE ts <= '2025-08-01' ORDER BY ts;
EXPLAIN SELECT count() FROM pruning_t WHERE ts <= '2025-08-01';

-- Test 4: PREWHERE participates in pruning.
SELECT * FROM pruning_t PREWHERE ts > '2025-10-01' ORDER BY ts;
EXPLAIN SELECT count() FROM pruning_t PREWHERE ts > '2025-10-01';

-- Test 5: All segments pruned — getQueryProcessingStage returns FetchColumns,
-- planner inserts ReadNothing, AggregatingTransform synthesizes the default row.
-- The meaningful answer here is "zero rows", so `count()` is the right shape.
SELECT count() FROM pruning_t WHERE ts > '2025-12-01' AND ts <= '2025-08-01';
EXPLAIN SELECT count() FROM pruning_t WHERE ts > '2025-12-01' AND ts <= '2025-08-01';

-- {echoOff}

-- Test 6: three-segment table; cold + middle pruned, only hot kept.
CREATE TABLE local_warm (ts DateTime, value UInt64) ENGINE = MergeTree ORDER BY ts;
INSERT INTO local_warm VALUES ('2025-09-15', 5), ('2025-09-25', 6);

CREATE TABLE pruning_t3
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('127.0.0.1:9000', currentDatabase(), 'local_warm'),
        ts > hybridParam('hybrid_watermark_cold', 'DateTime')
        AND ts <= hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('127.0.0.1:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_cold', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-10-01', hybrid_watermark_cold = '2025-09-01'
AS local_hot;

CREATE TABLE pruning_or
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'local_hot'),
        ts > hybridParam('hybrid_watermark_hot', 'DateTime'),
    remote('127.0.0.1:9000', currentDatabase(), 'local_cold'),
        ts <= hybridParam('hybrid_watermark_hot', 'DateTime')
)
SETTINGS hybrid_watermark_hot = '2025-09-01'
AS local_hot;

CREATE TABLE dim (id UInt64, label String) ENGINE = MergeTree ORDER BY id;
INSERT INTO dim VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

-- {echoOn}

-- Test 6: three segments, only hot survives.
SELECT * FROM pruning_t3 WHERE ts > '2025-10-01' ORDER BY ts;
EXPLAIN SELECT count() FROM pruning_t3 WHERE ts > '2025-10-01';

-- Test 7: OR alternative is not a mandatory constraint — hot survives via the OR.
SELECT * FROM pruning_or WHERE (value = 1 OR value = 2) AND ts > '2025-10-01' ORDER BY ts;
EXPLAIN SELECT count() FROM pruning_or WHERE (value = 1 OR value = 2) AND ts > '2025-10-01';

-- Test 8: JOIN — pruner conservatively skips, both segments planned. EXPLAIN is omitted
-- because JOIN-side ordering depends on randomized settings the test harness cycles
-- through (e.g. query_plan_join_swap_table); we verify by projecting the joined columns.
SELECT t.ts, t.value, d.label
FROM pruning_t AS t
INNER JOIN dim AS d ON t.value = d.id
WHERE d.id > 1 AND t.ts <= '2025-08-01'
ORDER BY t.ts;

-- Test 9: SELECT alias shadows a Hybrid column used by segment predicates. With default
-- prefer_column_name_to_alias=0 the WHERE's `ts` resolves to the alias expression (a
-- constant true for every row); if the pruner mistakenly treated the unresolved `ts` as
-- the Hybrid column it would prune the cold segment (`ts <= '2025-09-01'`) and silently
-- drop those rows. All 4 rows must survive — projecting `value` confirms which rows.
SELECT ts, value FROM (SELECT toDateTime('2025-11-01') AS ts, value FROM pruning_t WHERE ts > '2025-10-01') ORDER BY value;

-- {echoOff}

DROP TABLE IF EXISTS dim SYNC;
DROP TABLE IF EXISTS pruning_or SYNC;
DROP TABLE IF EXISTS pruning_t3 SYNC;
DROP TABLE IF EXISTS pruning_t SYNC;
DROP TABLE IF EXISTS local_hot SYNC;
DROP TABLE IF EXISTS local_cold SYNC;
DROP TABLE IF EXISTS local_warm SYNC;
