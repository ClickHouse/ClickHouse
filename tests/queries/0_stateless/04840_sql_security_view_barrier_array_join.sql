-- A `SQL SECURITY DEFINER` / `NONE` view whose plan contains `ARRAY JOIN` is an optimization
-- barrier: an empty array hides its row, so nothing from the outer query may be evaluated below
-- the `ArrayJoinStep`. `tryLiftUpArrayJoin` used to split the sealed converting step (non-trivial
-- here because of the explicit view column types) and rebuild both halves as fresh unmarked
-- steps, after which the invoker's predicate sank below the `ARRAY JOIN` and was evaluated on
-- the hidden row.

SET query_plan_lift_up_array_join = 1, query_plan_filter_push_down = 1, query_plan_merge_expressions = 1, enable_parallel_replicas = 0, max_threads = 1;

DROP TABLE IF EXISTS t04840;
CREATE TABLE t04840 (key UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY key;
INSERT INTO t04840 SELECT number, if(number = 42, [], [number, number + 1]) FROM numbers(100);

CREATE VIEW v04840_invoker (key UInt32, item UInt64) SQL SECURITY INVOKER AS
    SELECT key, item FROM t04840 ARRAY JOIN arr AS item;
CREATE VIEW v04840_definer (key UInt32, item UInt64) DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT key, item FROM t04840 ARRAY JOIN arr AS item;

SET enable_analyzer = 1;

-- The `INVOKER` view stays fully optimizable: the outer predicate descends below the
-- `ARRAY JOIN` and observes the row the empty array hides. This is the positive control
-- proving that the oracle discriminates.
SELECT 'invoker, analyzer:';
SELECT count() FROM v04840_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The `DEFINER` view is a barrier: the predicate stays above the `ARRAY JOIN`, so it never sees
-- the hidden row.
SELECT 'definer, analyzer:', count() FROM v04840_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;

-- Plan shape: in the definer's plan, every `throwIf` line comes before the `ARRAY JOIN` line.
SELECT 'definer keeps the predicate above the ARRAY JOIN:',
    max(if(explain LIKE '%throwIf%', rn, 0)) < min(if(explain LIKE '%ArrayJoin%', rn, 1000000))
FROM
(
    SELECT explain, rowNumberInAllBlocks() AS rn
    FROM (EXPLAIN actions = 1, compact = 0 SELECT * FROM v04840_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0)
);

SET enable_analyzer = 0;

SELECT 'invoker, legacy analyzer:';
SELECT count() FROM v04840_invoker WHERE throwIf(key = 42, 'DISCLOSED') = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'definer, legacy analyzer:', count() FROM v04840_definer WHERE throwIf(key = 42, 'DISCLOSED') = 0;

SET enable_analyzer = DEFAULT;

-- The barrier only drops the optimization, never the result.
SELECT 'definer results:', count(), min(key), max(key) FROM v04840_definer WHERE key % 2 = 0;

DROP VIEW v04840_invoker;
DROP VIEW v04840_definer;
DROP TABLE t04840;
