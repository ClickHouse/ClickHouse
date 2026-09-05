-- The constraint optimization asks its comparison graph whether an atom of the `WHERE` is always
-- true. For an atom that is not a comparison at all - `a IN (5, 8, 12)` - nothing follows either way,
-- but the arguments were compared first, and no order puts an `IN` list and the scalar bound of a
-- constraint together: the query failed with `TYPE_MISMATCH`.

DROP TABLE IF EXISTS t_constraint_in;
CREATE TABLE t_constraint_in (a Int64, s String, CONSTRAINT c_a ASSUME a <= 100) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_constraint_in SELECT number, toString(number) FROM numbers(50);

SELECT 'an IN list over the constrained column';
SELECT count() FROM t_constraint_in WHERE a IN (5, 8, 12) SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1;
SELECT count() FROM t_constraint_in WHERE a IN (5, 8, 12) SETTINGS convert_query_to_cnf = 0, optimize_using_constraints = 0;

SELECT 'a NOT IN list';
SELECT count() FROM t_constraint_in WHERE a NOT IN (5, 8, 12) SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1;
SELECT count() FROM t_constraint_in WHERE a NOT IN (5, 8, 12) SETTINGS convert_query_to_cnf = 0, optimize_using_constraints = 0;

SELECT 'a tuple IN list';
SELECT count() FROM t_constraint_in WHERE (a, s) IN ((5, '5'), (8, 'x')) SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1;
SELECT count() FROM t_constraint_in WHERE (a, s) IN ((5, '5'), (8, 'x')) SETTINGS convert_query_to_cnf = 0, optimize_using_constraints = 0;

SELECT 'an IN list under OR next to a comparison';
SELECT count() FROM t_constraint_in WHERE a IN (5, 8, 12) OR a > 40 SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1;
SELECT count() FROM t_constraint_in WHERE a IN (5, 8, 12) OR a > 40 SETTINGS convert_query_to_cnf = 0, optimize_using_constraints = 0;

SELECT 'the same through the old analyzer';
SELECT count() FROM t_constraint_in WHERE a IN (5, 8, 12) SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1, enable_analyzer = 0;

-- The constraint is still used: it proves `a > 100` always false, so the condition folds to a constant.
SELECT 'the constraint still applies';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_constraint_in WHERE a > 100 SETTINGS convert_query_to_cnf = 1, optimize_using_constraints = 1)
WHERE explain LIKE '%constant_value: UInt64_0%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_constraint_in WHERE a > 100 SETTINGS convert_query_to_cnf = 0, optimize_using_constraints = 0)
WHERE explain LIKE '%function_name: greater%';

DROP TABLE t_constraint_in;
