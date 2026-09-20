-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A query that references a dictionary of this server (`dictGet` and its variations, `dictHas`, a read of the dictionary
-- as a table) under `make_distributed_plan` falls back to local execution: the dictionary is an object of the initiator,
-- a worker task that receives the expression cannot resolve it (`Dictionary (...) not found` from
-- `ReadFromDistributedPlanSource`). The function is looked for in every expression a fragment carries: expression and
-- filter steps, the join expression, the filters pushed into the `MergeTree` read (prewhere).
-- Each row prints the number of distributed-plan tasks spawned under its `log_comment` (initiator and tasks inherit it)
-- and the reason the initiator logged; the positive control without a dictionary must stay distributed.

SET make_distributed_plan = 1;
-- The CI users profile sets a global GROUP BY limit, which is a fallback reason of its own; lifted so that the
-- dictionary is the only candidate reason.
SET max_rows_to_group_by = 0;
SET prefer_localhost_replica = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
-- Randomized by the test runner; it would rewrite `dictGet(...) = const` into an `IN` over the dictionary table and take
-- the function out of the plan.
SET optimize_inverse_dictionary_lookup = 0;

DROP DICTIONARY IF EXISTS d_ddf;
DROP TABLE IF EXISTS t_ddf;
DROP TABLE IF EXISTS src_ddf;

-- Created first: its metadata_modification_time fences the log checks to this run.
CREATE TABLE t_ddf (k UInt64, v String, created DateTime) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ddf SELECT number, toString(number), toDateTime('2026-01-01 00:00:00') + number FROM numbers(1000);
CREATE TABLE src_ddf (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src_ddf SELECT number, concat('n', toString(number)) FROM numbers(1000);
CREATE DICTIONARY d_ddf (k UInt64, name String) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 'src_ddf' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(0);
SYSTEM RELOAD DICTIONARY d_ddf;

-- 1: dictGet in the projection.
SELECT k, dictGet(d_ddf, 'name', k) AS name FROM t_ddf WHERE k < 3 ORDER BY k SETTINGS log_comment = '05219_ddf_1_projection';
-- 2: dictGet in the filter.
SELECT count() FROM t_ddf WHERE dictGet(d_ddf, 'name', k) = 'n5' SETTINGS log_comment = '05219_ddf_2_filter';
-- 3: dictHas.
SELECT count() FROM t_ddf WHERE dictHas(d_ddf, k) SETTINGS log_comment = '05219_ddf_3_dict_has';
-- 4: the reported shape: a scalar subquery, dictGet in the projection, ORDER BY ... LIMIT. The scalar subquery is its own
-- unit with its own decision and no dictionary function, so it still distributes (its tasks are the ones counted);
-- the outer plan falls back.
WITH (SELECT v FROM t_ddf WHERE k < 100 ORDER BY k DESC LIMIT 1) AS max_v
SELECT k, v, max_v, dictGet(d_ddf, 'name', k) AS name FROM t_ddf WHERE k < 100 AND 1 = 1 ORDER BY created DESC LIMIT 1
    SETTINGS log_comment = '05219_ddf_4_scalar_order_limit';
-- 5: the dictionary read as a table (a `ReadFromStorage`, not shippable).
SELECT count() FROM d_ddf SETTINGS log_comment = '05219_ddf_5_read_dictionary_table';
-- 6: dictGetOrDefault in the prewhere of the read (the filter is moved there when the runner allows it).
SELECT count() FROM t_ddf PREWHERE dictGetOrDefault(d_ddf, 'name', k, 'none') = 'n7' SETTINGS log_comment = '05219_ddf_6_prewhere';
-- 7: dictGet in a join condition.
SELECT count() FROM t_ddf AS a INNER JOIN src_ddf AS b ON dictGet(d_ddf, 'name', a.k) = b.name AND a.k < 10
    SETTINGS log_comment = '05219_ddf_7_join_condition';

-- Strict mode: the fallback is asserted, the query throws instead of running locally.
SELECT k, dictGet(d_ddf, 'name', k) AS name FROM t_ddf WHERE k < 3 ORDER BY k
    SETTINGS distributed_plan_fallback_to_local_execution = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Positive control: the same read without a dictionary stays distributed.
SELECT k, v FROM t_ddf WHERE k < 3 ORDER BY k SETTINGS log_comment = '05219_ddf_positive_no_dictionary';

SYSTEM FLUSH LOGS query_log, text_log;

WITH (SELECT metadata_modification_time FROM system.tables WHERE database = currentDatabase() AND name = 't_ddf') AS run_start,
    (SELECT groupArray(query_id) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment LIKE '05219_ddf_%' AND event_time >= run_start) AS run_ids,
    family AS (
        SELECT query_id, log_comment, is_initial_query, ProfileEvents['DistributedPlanRemoteTasks'] AS tasks
        FROM system.query_log
        WHERE type = 'QueryFinish' AND has(run_ids, initial_query_id) AND log_comment LIKE '05219_ddf_%'),
    reasons AS (
        SELECT f.log_comment,
            replaceRegexpOne(replaceRegexpOne(extract(t.message, 'falling back to local execution: (.*)$'),
                '^make_distributed_plan (does not support |cannot distribute this query: it contains the step )', ''),
                ' which could not execute remotely$', '') AS reason
        FROM system.text_log AS t INNER JOIN family AS f ON t.query_id = f.query_id
        WHERE t.event_date >= toDate(run_start) AND t.event_time >= run_start AND f.is_initial_query
            AND t.logger_name = 'makeDistributedPlan' AND t.message LIKE '%falling back to local execution%')
SELECT f.log_comment, sum(f.tasks) AS remote_tasks,
    (SELECT arraySort(groupUniqArray(reason)) FROM reasons WHERE reasons.log_comment = f.log_comment) AS initiator_fell_back_on
FROM family AS f
GROUP BY f.log_comment
ORDER BY f.log_comment;

DROP DICTIONARY d_ddf;
DROP TABLE t_ddf;
DROP TABLE src_ddf;
