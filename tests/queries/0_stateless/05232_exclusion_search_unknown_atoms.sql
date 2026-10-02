-- A condition on non-key columns must not make the generic exclusion search split ranges down to single
-- marks: the selected marks and the number of steps must be as for the key part of the condition alone.

DROP TABLE IF EXISTS t_exclusion_search_unknown_atoms;

CREATE TABLE t_exclusion_search_unknown_atoms (a UInt8, b UInt32, c UInt32, s String)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8;

-- Three values of `a`, so that a condition on `b` uses the exclusion search, not the binary search.
INSERT INTO t_exclusion_search_unknown_atoms
SELECT number % 3, intDiv(number, 3), number, toString(number) FROM numbers(30000);

OPTIMIZE TABLE t_exclusion_search_unknown_atoms FINAL;

-- The condition on `b` alone needs about 200 steps, so the budget of 500 must not be reached.
-- Every query runs with and without the budget; results and selected marks must match.
-- `sum` disables the exact-count optimization.

SET merge_tree_coarse_index_granularity = 8;
-- The query condition cache would pre-split the ranges for repeated predicates.
SET use_query_condition_cache = 0;

-- { echoOn }

SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05232_unknown_atoms range budget';
SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05232_unknown_atoms range unlimited';

SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000 AND c % 7 = 1
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05232_unknown_atoms and_unknown budget';
SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000 AND c % 7 = 1
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05232_unknown_atoms and_unknown unlimited';

SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000 AND NOT startsWith(s, '1')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05232_unknown_atoms and_not_unknown budget';
SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000 AND NOT startsWith(s, '1')
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05232_unknown_atoms and_not_unknown unlimited';

SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE (b < 3000 OR c % 7 = 1) AND b BETWEEN 2000 AND 7000
SETTINGS merge_tree_generic_exclusion_search_max_steps = 500, log_comment = '05232_unknown_atoms or_unknown budget';
SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE (b < 3000 OR c % 7 = 1) AND b BETWEEN 2000 AND 7000
SETTINGS merge_tree_generic_exclusion_search_max_steps = 0, log_comment = '05232_unknown_atoms or_unknown unlimited';

SYSTEM FLUSH LOGS query_log;

WITH (
    SELECT max(ProfileEvents['SelectedMarks']) FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05232_unknown_atoms range unlimited'
) AS marks_of_range_alone
SELECT
    splitByChar(' ', log_comment)[2] AS test_case,
    maxIf(ProfileEvents['IndexGenericExclusionSearchStepLimitReached'], splitByChar(' ', log_comment)[3] = 'budget') AS step_limit_reached_with_budget,
    maxIf(ProfileEvents['SelectedMarks'], splitByChar(' ', log_comment)[3] = 'budget')
        = maxIf(ProfileEvents['SelectedMarks'], splitByChar(' ', log_comment)[3] = 'unlimited') AS same_marks_as_unlimited,
    maxIf(ProfileEvents['SelectedMarks'], splitByChar(' ', log_comment)[3] = 'unlimited') = marks_of_range_alone AS same_marks_as_range_alone
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05232_unknown_atoms %'
GROUP BY test_case
ORDER BY test_case;

-- Atoms assumed true during the search must not make ranges exact for the exact-count optimization.
SELECT count() FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000 AND c % 7 = 1
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 500;
SELECT count() FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000 AND c % 7 = 1
SETTINGS optimize_use_projections = 0, optimize_use_implicit_projections = 0, merge_tree_generic_exclusion_search_max_steps = 500;
SELECT count() FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1, merge_tree_generic_exclusion_search_max_steps = 500;
SELECT count() FROM t_exclusion_search_unknown_atoms
WHERE b BETWEEN 2000 AND 7000
SETTINGS optimize_use_projections = 0, optimize_use_implicit_projections = 0, merge_tree_generic_exclusion_search_max_steps = 500;

-- An atom under `NOT` is assumed false, otherwise the rows with `b < 2000` would be excluded.
SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE NOT (b < 2000 AND startsWith(s, '4')) AND b BETWEEN 1000 AND 3000;
SELECT count(), sum(c) FROM t_exclusion_search_unknown_atoms
WHERE NOT (b < 2000 AND startsWith(s, '4')) AND b BETWEEN 1000 AND 3000
SETTINGS use_primary_key = 0;

-- { echoOff }

DROP TABLE t_exclusion_search_unknown_atoms;
