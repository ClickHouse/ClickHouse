-- Tags: no-old-analyzer

-- Verifies the part of `system.query_log.query_plan` that a plain SELECT never reaches: the
-- sub-plans a step returns from `getChildPlans`. A `Merge` table produces one per underlying
-- table, and they are a separate naming scope, so this covers the per-plan pretty-name lookup and
-- the suppression of the root `Output` on anything that is not the plan the caller asked about.
--
-- Rows are matched on `log_comment` rather than on the query text, so that the query itself stays
-- exactly what is being described, and on `currentDatabase()` so a parallel run cannot be seen.

DROP TABLE IF EXISTS mrg_a;
DROP TABLE IF EXISTS mrg_b;
DROP TABLE IF EXISTS mrg;

CREATE TABLE mrg_a (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mrg_b (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO mrg_a SELECT number FROM numbers(1000);
INSERT INTO mrg_b SELECT number + 1000 FROM numbers(1000);
CREATE TABLE mrg (k UInt64) ENGINE = Merge(currentDatabase(), '^mrg_[ab]$');

SET log_query_plans = 1;
SELECT count() FROM mrg WHERE k % 3 = 0 SETTINGS log_comment = '05046_merge' FORMAT Null;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    'child_plans',
    count(),
    anyLast(isValidJSON(toJSONString(query_plan))),
    -- The step that owns the sub-plans, and one entry per underlying table below it. Without the
    -- recursion into getChildPlans the tree would stop at ReadFromMerge.
    anyLast(position(toJSONString(query_plan), 'ReadFromMerge"')) > 0,
    anyLast(countSubstrings(toJSONString(query_plan), '"Node Type":"ReadFromMergeTree"')),
    -- Written once, for the plan the caller asked about. A sub-plan root must not repeat it, which
    -- is the whole reason explainPlan knows whether it is rendering a child plan.
    anyLast(countSubstrings(toJSONString(query_plan), '"Output":')),
    -- Both sub-plans describe their own filter, and they do it through the names captured before
    -- the pipeline was built: an unresolved name would read as a column identifier, not as `MOD`.
    anyLast(countSubstrings(toJSONString(query_plan), 'Filter column: ')),
    anyLast(position(toJSONString(query_plan), 'MOD 3 = 0')) > 0,
    -- Statistics reach the steps inside a sub-plan too.
    anyLast(countSubstrings(toJSONString(query_plan), '"WallClockTimeNs"')) > 2
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05046_merge';

DROP TABLE mrg;
DROP TABLE mrg_a;
DROP TABLE mrg_b;
