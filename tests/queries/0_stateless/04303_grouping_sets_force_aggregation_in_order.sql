-- Tags: no-random-settings

-- Direct path: old-analyzer + force_aggregation_in_order + GROUPING SETS.
SET enable_analyzer = 0, force_aggregation_in_order = 1;

SELECT 1 GROUP BY GROUPING SETS ((1), (1)) ORDER BY 1;
SELECT number FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY number;

-- Indirect path: ALTER UPDATE pushes the GROUPING SETS subquery through the old analyzer
-- via `replaceNonDeterministicToScalars`. The two-row subquery still fails, but with the
-- user-facing INCORRECT_RESULT_OF_SCALAR_SUBQUERY rather than a LOGICAL_ERROR abort.
DROP TABLE IF EXISTS t_04303;
CREATE TABLE t_04303 (c0 String) ENGINE = Memory;

SET enable_analyzer = 1, mutations_execute_subqueries_on_initiator = 1, force_aggregation_in_order = 1;

ALTER TABLE t_04303 UPDATE c0 = (SELECT 1 GROUP BY GROUPING SETS ((1), (1))) WHERE TRUE; -- { serverError INCORRECT_RESULT_OF_SCALAR_SUBQUERY }

DROP TABLE t_04303;
