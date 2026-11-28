-- When a row policy (`apply_row_policy_after_final`) or PREWHERE (`apply_prewhere_after_final`)
-- is deferred until after the FINAL merge, the merge sees unfiltered rows. Pushing the LIMIT into
-- the merge would then stop after the first `N` groups and the deferred filter could reject all of
-- them, so `optimize_final_limit_pushdown` must not apply. The results must match the ones with
-- the pushdown disabled, and `EXPLAIN PIPELINE` must not show the pushed-down limit.

DROP ROW POLICY IF EXISTS p_final_limit_deferred ON t_final_limit_deferred;
DROP TABLE IF EXISTS t_final_limit_deferred;

CREATE TABLE t_final_limit_deferred (key UInt64, v SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY key;

-- After FINAL, `v = 2 * key`, so `v >= 100` keeps only keys 50 and above,
-- while each row of a single insert has `v = key` and passes the filter only for keys 100 and above.
SYSTEM STOP MERGES t_final_limit_deferred;
INSERT INTO t_final_limit_deferred SELECT number, number FROM numbers(200);
INSERT INTO t_final_limit_deferred SELECT number, number FROM numbers(200);

SELECT 'prewhere after final, pushdown off';
SELECT key, v FROM t_final_limit_deferred FINAL PREWHERE v >= 100 ORDER BY key LIMIT 3
SETTINGS apply_prewhere_after_final = 1, optimize_final_limit_pushdown = 0;

SELECT 'prewhere after final, pushdown on';
SELECT key, v FROM t_final_limit_deferred FINAL PREWHERE v >= 100 ORDER BY key LIMIT 3
SETTINGS apply_prewhere_after_final = 1, optimize_final_limit_pushdown = 1, optimize_read_in_order = 1;

SELECT 'prewhere after final, pushed-down limit in pipeline';
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT key, v FROM t_final_limit_deferred FINAL PREWHERE v >= 100 ORDER BY key LIMIT 3
    SETTINGS apply_prewhere_after_final = 1, optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

CREATE ROW POLICY p_final_limit_deferred ON t_final_limit_deferred USING v >= 100 TO ALL;

SELECT 'row policy after final, pushdown off';
SELECT key, v FROM t_final_limit_deferred FINAL ORDER BY key LIMIT 3
SETTINGS apply_row_policy_after_final = 1, optimize_final_limit_pushdown = 0;

SELECT 'row policy after final, pushdown on';
SELECT key, v FROM t_final_limit_deferred FINAL ORDER BY key LIMIT 3
SETTINGS apply_row_policy_after_final = 1, optimize_final_limit_pushdown = 1, optimize_read_in_order = 1;

SELECT 'row policy after final, pushed-down limit in pipeline';
SELECT count()
FROM (
    EXPLAIN PIPELINE
    SELECT key, v FROM t_final_limit_deferred FINAL ORDER BY key LIMIT 3
    SETTINGS apply_row_policy_after_final = 1, optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit%';

DROP ROW POLICY p_final_limit_deferred ON t_final_limit_deferred;

-- Sanity check: without deferred filters the limit is pushed down.
SELECT 'no deferred filters, pushed-down limit in pipeline';
SELECT count() > 0
FROM (
    EXPLAIN PIPELINE
    SELECT key, v FROM t_final_limit_deferred FINAL ORDER BY key LIMIT 3
    SETTINGS optimize_final_limit_pushdown = 1, optimize_read_in_order = 1
)
WHERE explain LIKE '%Description: limit 3%';

DROP TABLE t_final_limit_deferred;
