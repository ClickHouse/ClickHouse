-- Verify that EXPLAIN with UNION and trailing SETTINGS round-trips correctly through formatting.
-- The bug: SETTINGS on the EXPLAIN was consumed by the last SELECT during re-parse
-- because the last SELECT in the UNION chain was not parenthesized.
-- formatQuerySingleLine verifies correct parenthesization.

SELECT formatQuerySingleLine('EXPLAIN description = 1 (SELECT 1) EXCEPT (SELECT 1) UNION (SELECT 1) SETTINGS except_default_mode = \'\'');
SELECT formatQuerySingleLine('EXPLAIN (SELECT 1) UNION ALL (SELECT 2) SETTINGS max_threads = 1');
SELECT formatQuerySingleLine('EXPLAIN SYNTAX (SELECT 1) UNION (SELECT 2) SETTINGS max_threads = 1');
