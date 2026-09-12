-- `tryPushDownVolumeReducingFunction` splices the invoker's `length` / `empty` / `notEmpty` below a
-- `Filter` that reads their argument. When that filter decides which rows a `SQL SECURITY DEFINER` /
-- `NONE` view exposes, the pushed function would run on the hidden rows before the barrier: the rows
-- are still filtered out, but their payload is scanned first. The pass must refuse a barrier step.

-- The plan-shape assertions describe the step layout produced by the analyzer; the pass needs the
-- expression and the filter to be adjacent, which `query_plan_merge_expressions` produces.
SET enable_analyzer = 1;
SET query_plan_merge_expressions = 1, query_plan_push_down_volume_reducing_functions = 1;
SET optimize_functions_to_subcolumns = 0, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, enable_parallel_replicas = 0;
SET query_plan_max_step_description_length = 10000;

DROP TABLE IF EXISTS secrets_05102;
CREATE TABLE secrets_05102 (owner String, secret String) ENGINE = MergeTree ORDER BY owner;
INSERT INTO secrets_05102 VALUES ('me', 'visible'), ('someone_else', 'HIDDEN');

-- The stored query projects every row; a row policy on the view is the only security boundary, and
-- it is applied as a `Filter` step above the view's subplan that reads `secret` itself.
CREATE VIEW view_definer_05102 DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM secrets_05102;
CREATE VIEW view_none_05102 SQL SECURITY NONE AS SELECT owner, secret FROM secrets_05102;
-- The same view without a security context switch, as the optimization baseline.
CREATE VIEW view_invoker_05102 SQL SECURITY INVOKER AS SELECT owner, secret FROM secrets_05102;

CREATE ROW POLICY policy_definer_05102 ON view_definer_05102 FOR SELECT USING secret != 'HIDDEN' TO ALL;
CREATE ROW POLICY policy_none_05102 ON view_none_05102 FOR SELECT USING secret != 'HIDDEN' TO ALL;
CREATE ROW POLICY policy_invoker_05102 ON view_invoker_05102 FOR SELECT USING secret != 'HIDDEN' TO ALL;

SELECT 'invoker view: length pushed below the policy filter:', countIf(explain LIKE '%[volume-reducing functions]%')
    FROM (EXPLAIN description = 1, actions = 0 SELECT length(secret) FROM view_invoker_05102);

SELECT 'definer view: length pushed below the policy filter:', countIf(explain LIKE '%[volume-reducing functions]%')
    FROM (EXPLAIN description = 1, actions = 0 SELECT length(secret) FROM view_definer_05102);
SELECT 'none view: length pushed below the policy filter:', countIf(explain LIKE '%[volume-reducing functions]%')
    FROM (EXPLAIN description = 1, actions = 0 SELECT length(secret) FROM view_none_05102);

SELECT 'definer view: notEmpty pushed below the policy filter:', countIf(explain LIKE '%[volume-reducing functions]%')
    FROM (EXPLAIN description = 1, actions = 0 SELECT notEmpty(secret) FROM view_definer_05102);

-- The barrier only drops the optimization, never the result.
SELECT 'definer view results:', groupArray(length(secret)) FROM view_definer_05102;
SELECT 'invoker view results:', groupArray(length(secret)) FROM view_invoker_05102;

DROP ROW POLICY policy_definer_05102 ON view_definer_05102;
DROP ROW POLICY policy_none_05102 ON view_none_05102;
DROP ROW POLICY policy_invoker_05102 ON view_invoker_05102;
DROP VIEW view_definer_05102;
DROP VIEW view_none_05102;
DROP VIEW view_invoker_05102;
DROP TABLE secrets_05102;
