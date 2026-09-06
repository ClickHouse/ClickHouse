-- Tags: no-parallel, no-random-settings
--
-- `query_plan_optimize_join_order_randomize = 1` means "derive a seed", and the seed feeds
-- `getRandomizedStats`, which replaces the real relation statistics and therefore decides the join order.
-- The seed must be a function of the query, not of the individual plan construction: a query builds several
-- plans (one per scalar subquery, and under parallel replicas one per replica: a replica re-plans whether it
-- receives the query text or a serialized plan, because `JoinStepLogical::serialize` does not encode the
-- `optimized` flag, so the join-order rewrite runs again on the replica). When those plans disagree on the
-- join order, the leftmost relation differs, so a different table gets `requestReadingInOrder` and two
-- replicas announce different coordination modes for one stream, throwing `Coordination mode mismatch for
-- stream`.
--
-- `no-parallel` is required because the assertions read `system.text_log`, which is server-global.
-- `no-random-settings` is required because the runner randomizes `query_plan_optimize_join_order_randomize`
-- itself (`tests/clickhouse-test`), which would overwrite the value under test.

DROP TABLE IF EXISTS t1_04653;
DROP TABLE IF EXISTS t2_04653;
DROP TABLE IF EXISTS t3_04653;

CREATE TABLE t1_04653 (x UInt64, z UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t2_04653 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t3_04653 (y UInt64, z UInt64) ENGINE = MergeTree ORDER BY y;

INSERT INTO t1_04653 SELECT number, number FROM numbers(1000);
INSERT INTO t2_04653 SELECT number, number FROM numbers(500);
INSERT INTO t3_04653 SELECT number, number FROM numbers(200);

SET enable_analyzer = 1;
SET max_rows_to_read = 0; -- system.text_log can be really big
SET max_execution_time = 0;

-- Cell A: one query builds three plans (three scalar subqueries over the same join, each constructing its own
-- `QueryPlanOptimizationSettings`). All of them must use the same derived seed.
SELECT
    (SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z) AS a,
    (SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z) AS b,
    (SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z) AS c
SETTINGS query_plan_optimize_join_order_randomize = 1, query_plan_optimize_join_order_limit = 3, log_comment = '04653_cell_a';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT 'cell A one seed per query', uniqExact(extract(message, 'seed (\\d+)')) = 1
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04653_cell_a' AND type != 'QueryStart'
        AND initial_query_id = (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '04653_cell_a'
              AND is_initial_query AND type != 'QueryStart'
            ORDER BY event_time_microseconds DESC
            LIMIT 1));

-- The seed must be the DERIVED value, not merely one value: a constant seed would also be unique within a
-- query and across replicas.
-- `system.query_log` is server-global and append-only, and the runner re-runs a failed test in the supplied
-- database without cleaning it (a transient failure such as the global memory tracker firing is retried), so one
-- database can hold several attempts of this test. Both the expected side and the observed side below are
-- therefore scoped to the NEWEST initiator: an unscoped observed side would see one derived seed per attempt, and
-- an unordered single-row pick on the expected side could hash a previous attempt's query id.
SELECT 'cell A seed equals hash of initial query id', (
    SELECT groupUniqArray(extract(message, 'seed (\\d+)'))
    FROM system.text_log
    WHERE logger_name = 'QueryPlanOptimizationSettings'
      AND message LIKE '%random seed%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04653_cell_a' AND type != 'QueryStart'
            AND initial_query_id = (
                SELECT query_id FROM system.query_log
                WHERE current_database = currentDatabase() AND log_comment = '04653_cell_a'
                  AND is_initial_query AND type != 'QueryStart'
                ORDER BY event_time_microseconds DESC
                LIMIT 1))
) = (
    SELECT [toString(greatest(sipHash64(query_id), 2))]
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04653_cell_a'
      AND is_initial_query AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC
    LIMIT 1);

-- Cell A must be non-vacuous: the query really did construct more than one plan.
SELECT 'cell A saw several plan constructions', count() > 1
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04653_cell_a' AND type != 'QueryStart'
        AND initial_query_id = (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '04653_cell_a'
              AND is_initial_query AND type != 'QueryStart'
            ORDER BY event_time_microseconds DESC
            LIMIT 1));

-- Cell B (must-not-change control): an explicit seed above 1 is still used verbatim, and different explicit
-- seeds still produce different join orders, so the randomization keeps its coverage.
SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z
SETTINGS query_plan_optimize_join_order_randomize = 12345, query_plan_optimize_join_order_limit = 3, log_comment = '04653_cell_b';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT 'cell B explicit seed used verbatim', groupUniqArray(extract(message, 'seed (\\d+)')) = ['12345']
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04653_cell_b' AND type != 'QueryStart'
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '04653_cell_b' AND is_initial_query));

-- The randomization must still explore different join orders, otherwise the fix would have silently turned the
-- feature off. Seeds 2 and 3 are used because the chosen order is a hash of (seed, relation index, table name),
-- so two arbitrary seeds can legitimately land on the same order for one particular set of table names: seeds
-- 12345 and 54321 do exactly that here. Seeds 2 and 3 are verified to differ for these table names.
-- The join-swap pin is required because both the functional runner and the stress runner randomize the setting,
-- and a forced swap changes the read order independently of the seed, which is what this row measures.
SELECT 'cell B distinct seeds still reorder', (
    SELECT groupArray(explain) FROM (
        EXPLAIN PLAN SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z
        SETTINGS query_plan_optimize_join_order_randomize = 2, query_plan_optimize_join_order_limit = 3, query_plan_join_swap_table = 'auto')
    WHERE explain ILIKE '%ReadFromMergeTree%') != (
    SELECT groupArray(explain) FROM (
        EXPLAIN PLAN SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z
        SETTINGS query_plan_optimize_join_order_randomize = 3, query_plan_optimize_join_order_limit = 3, query_plan_join_swap_table = 'auto')
    WHERE explain ILIKE '%ReadFromMergeTree%');

-- Cell B2 (must-not-change control): the default value 0 disables randomization entirely and is untouched.
SELECT count() FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z
SETTINGS query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 3, log_comment = '04653_cell_b2';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT 'cell B2 randomize=0 derives no seed', count() = 0
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04653_cell_b2' AND type != 'QueryStart'
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '04653_cell_b2' AND is_initial_query));

-- Cell C: the parallel-replicas path this fixes. Every replica re-plans the query and must derive the same
-- seed, so all of them choose the same join order and announce the same coordination mode.
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 0;

SELECT count() FROM (
    SELECT t1_04653.x, t1_04653.z, t2_04653.x, t2_04653.y, t3_04653.y, t3_04653.z
    FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x
    JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z
    ORDER BY ALL)
SETTINGS query_plan_optimize_join_order_randomize = 1, query_plan_optimize_join_order_limit = 3, log_comment = '04653_cell_c';

SYSTEM FLUSH LOGS query_log, text_log;

SELECT 'cell C one seed across replicas', uniqExact(extract(message, 'seed (\\d+)')) = 1
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04653_cell_c' AND type != 'QueryStart'
        AND initial_query_id = (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c'
              AND is_initial_query AND type != 'QueryStart'
            ORDER BY event_time_microseconds DESC
            LIMIT 1));

-- The replicas must have agreed on the DERIVED value, not merely on some value. Each replica has its own
-- `query_id`, so hashing the initial one is what makes them agree.
SELECT 'cell C seed equals hash of initial query id', (
    SELECT groupUniqArray(extract(message, 'seed (\\d+)'))
    FROM system.text_log
    WHERE logger_name = 'QueryPlanOptimizationSettings'
      AND message LIKE '%random seed%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04653_cell_c' AND type != 'QueryStart'
            AND initial_query_id = (
                SELECT query_id FROM system.query_log
                WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c'
                  AND is_initial_query AND type != 'QueryStart'
                ORDER BY event_time_microseconds DESC
                LIMIT 1))
) = (
    SELECT [toString(greatest(sipHash64(query_id), 2))]
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c'
      AND is_initial_query AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC
    LIMIT 1);

-- Cell C must be non-vacuous: the query really did construct more than one plan.
SELECT 'cell C saw several plan constructions', uniqExact(query_id) > 1
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND query_id IN (
      SELECT query_id FROM system.query_log
      WHERE log_comment = '04653_cell_c' AND type != 'QueryStart'
        AND initial_query_id = (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c'
              AND is_initial_query AND type != 'QueryStart'
            ORDER BY event_time_microseconds DESC
            LIMIT 1));

-- Cell C non-vacuity, part 1: more than one plan construction carried this query's derived seed.
-- The scope is the seed value itself, computed from the initiator's own `query_log` row, which is
-- always locally visible; joining the replicas' rows instead would race their `query_log` flush
-- (`SYSTEM FLUSH LOGS` has no cross-replica barrier).
SELECT 'cell C several plans carried the derived seed', uniqExact(query_id) >= 2
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND extract(message, 'seed (\\d+)') = (
      SELECT toString(greatest(sipHash64(query_id), 2))
      FROM system.query_log
      WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c'
        AND is_initial_query AND type != 'QueryStart'
      ORDER BY event_time_microseconds DESC
      LIMIT 1);

-- Cell C non-vacuity, part 2: the configured fan-out really happened, so the multi-replica half of
-- the coordination contract is exercised rather than assumed. The population that carries the
-- derived seed is one initiator plan construction plus one per replica, so at
-- `max_parallel_replicas = 3` the count is 4. This row is deliberately coupled to that setting: if
-- the fixture's replica count is ever changed, this row must be updated with it.
SELECT 'cell C fanned out to three replicas', uniqExact(query_id) = 4
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND extract(message, 'seed (\\d+)') = (
      SELECT toString(greatest(sipHash64(query_id), 2))
      FROM system.query_log
      WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c'
        AND is_initial_query AND type != 'QueryStart'
      ORDER BY event_time_microseconds DESC
      LIMIT 1);

-- Cell C2: the serialized-plan replica path. With a local plan the initiator ships a serialized
-- plan to the replicas (`ClusterProxy/executeQuery.cpp:893-897`), and the receiver re-optimizes it
-- (`executeQuery.cpp:1980`) because `JoinStepLogical::serialize` does not encode the `optimized`
-- flag, so the join-order rewrite runs again there and must derive the same seed.
SELECT count() FROM (
    SELECT t1_04653.x, t1_04653.z, t2_04653.x, t2_04653.y, t3_04653.y, t3_04653.z
    FROM t1_04653 LEFT JOIN t2_04653 ON t1_04653.x = t2_04653.x
    JOIN t3_04653 ON t2_04653.y = t3_04653.y AND t1_04653.z = t3_04653.z
    ORDER BY ALL)
SETTINGS query_plan_optimize_join_order_randomize = 1, query_plan_optimize_join_order_limit = 3,
    parallel_replicas_local_plan = 1, serialize_query_plan = 1, log_comment = '04653_cell_c2';

SYSTEM FLUSH LOGS query_log, text_log;

-- The scope is the query, not the seed value: a seed-filtered population cannot observe a plan
-- construction that derived a DIFFERENT seed, so it could never fail on the disagreement this change is
-- about. Scoping by `initial_query_id` and comparing the whole set against the single derived value
-- asserts both halves at once: the constructions agreed, and they agreed on the derived value.
SELECT 'cell C2 serialized plan derives the same seed', (
    SELECT groupUniqArray(extract(message, 'seed (\\d+)'))
    FROM system.text_log
    WHERE logger_name = 'QueryPlanOptimizationSettings'
      AND message LIKE '%random seed%'
      AND query_id IN (
          SELECT query_id FROM system.query_log
          WHERE log_comment = '04653_cell_c2' AND type != 'QueryStart'
            AND initial_query_id = (
                SELECT query_id FROM system.query_log
                WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c2'
                  AND is_initial_query AND type != 'QueryStart'
                ORDER BY event_time_microseconds DESC
                LIMIT 1))
) = (
    SELECT [toString(greatest(sipHash64(query_id), 2))]
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c2'
      AND is_initial_query AND type != 'QueryStart'
    ORDER BY event_time_microseconds DESC
    LIMIT 1);

-- The replicas must really have received a serialized plan rather than the query text: that is what
-- distinguishes this cell from cell C. Without this row the cell stays green on the query-text
-- fallback, because a re-planning replica derives the SAME seed from the same propagated
-- `initial_query_id`, so the row above cannot tell the two paths apart. The discriminator is the
-- number of plan constructions: the deserialize path constructs `QueryPlanOptimizationSettings`
-- twice per receiver (`executeQuery.cpp:1980` and `:1987`; the second passes `do_optimize = false`,
-- but the object is built at the call site, so both log the seed), while a replica re-planning the
-- query text constructs it once (`InterpreterSelectQueryAnalyzer.cpp:403`). The initiator is
-- excluded because it constructs twice on both arms whenever `parallel_replicas_local_plan = 1`
-- (`InterpreterSelectQueryAnalyzer.cpp:192`, reached only by the initiator: `:184` returns early for
-- a secondary query), so its id is exactly the one participant that can double without
-- deserializing. Both subqueries read the initiator's own always-visible `query_log` row, so this
-- needs no replica `query_log` join, whose flush this test cannot force (`SYSTEM FLUSH LOGS` has no
-- cross-replica barrier).
SELECT 'cell C2 replicas received a serialized plan', count() > uniqExact(query_id)
FROM system.text_log
WHERE logger_name = 'QueryPlanOptimizationSettings'
  AND message LIKE '%random seed%'
  AND extract(message, 'seed (\\d+)') = (
      SELECT toString(greatest(sipHash64(query_id), 2))
      FROM system.query_log
      WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c2'
        AND is_initial_query AND type != 'QueryStart'
      ORDER BY event_time_microseconds DESC
      LIMIT 1)
  AND query_id != (
      SELECT query_id
      FROM system.query_log
      WHERE current_database = currentDatabase() AND log_comment = '04653_cell_c2'
        AND is_initial_query AND type != 'QueryStart'
      ORDER BY event_time_microseconds DESC
      LIMIT 1);

DROP TABLE t1_04653;
DROP TABLE t2_04653;
DROP TABLE t3_04653;
