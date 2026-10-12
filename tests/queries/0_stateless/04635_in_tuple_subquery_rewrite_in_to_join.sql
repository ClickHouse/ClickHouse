-- The analysis-time IN column-count validation must also fire when `rewrite_in_to_join`
-- lowers a non-constant `IN (subquery)` to EXISTS: without the shared check, the mismatch
-- would surface later as BAD_ARGUMENTS ("Cannot compare tuples of different sizes") from the
-- tuple comparison, making the reported error depend on an unrelated setting.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

SELECT count() FROM numbers(1) WHERE (number, number) IN (SELECT 1); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT count() FROM numbers(1) WHERE (number, number, number) IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }
SELECT count() FROM numbers(1) WHERE (number, number) NOT IN (SELECT 1, 2, 3); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

-- Valid matches keep working under the rewrite.
SELECT count() FROM numbers(3) WHERE (number, number) IN (SELECT 1, 1);
SELECT count() FROM numbers(3) WHERE number IN (SELECT 1);
