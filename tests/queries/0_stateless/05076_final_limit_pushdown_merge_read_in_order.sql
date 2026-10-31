-- `num_leading_fixed_sort_key_columns` in `InputOrderInfo` is a planning hint, not a property of the
-- produced order. `ReadFromMerge` compares the `InputOrderInfo` of its children to decide whether they
-- can be read in order together, and a child-local row policy on a leading sorting key column fixes
-- that column in one child only. Such children must still be treated as order-compatible.

DROP TABLE IF EXISTS t_merge_rio_1;
DROP TABLE IF EXISTS t_merge_rio_2;
DROP TABLE IF EXISTS t_merge_rio;

CREATE TABLE t_merge_rio_1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE t_merge_rio_2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_merge_rio_1 SELECT number % 3, number FROM numbers(30);
INSERT INTO t_merge_rio_2 SELECT number % 3, number FROM numbers(30);
CREATE TABLE t_merge_rio (a UInt64, b UInt64) ENGINE = Merge(currentDatabase(), '^t_merge_rio_[12]$');

CREATE ROW POLICY IF NOT EXISTS p_merge_rio ON t_merge_rio_1 FOR SELECT USING a = 1 TO ALL;

SET optimize_read_in_order = 1;

-- Keep the row policy as a `FilterStep` in the child plan, so that the optimizer sees `a` as fixed
-- for the first child only.
SELECT 'filter step, children read in order';
SELECT countIf(explain LIKE '%Read type: InOrder%'), countIf(explain LIKE '%Prefix sort description%')
FROM (EXPLAIN PLAN SELECT a, b FROM t_merge_rio ORDER BY a, b SETTINGS optimize_move_to_prewhere = 0);

SELECT 'prewhere, children read in order';
SELECT countIf(explain LIKE '%Read type: InOrder%'), countIf(explain LIKE '%Prefix sort description%')
FROM (EXPLAIN PLAN SELECT a, b FROM t_merge_rio ORDER BY a, b SETTINGS optimize_move_to_prewhere = 1);

SELECT 'results';
SELECT a, b FROM t_merge_rio ORDER BY a, b LIMIT 8 SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(a) FROM t_merge_rio;

DROP ROW POLICY IF EXISTS p_merge_rio ON t_merge_rio_1;
DROP TABLE t_merge_rio;
DROP TABLE t_merge_rio_1;
DROP TABLE t_merge_rio_2;
