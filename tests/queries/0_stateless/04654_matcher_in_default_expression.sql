-- A matcher in a column DEFAULT expression is rejected up front: assertNoMatcherNodes runs
-- before the analyzer resolves the expression, so the matcher is reported rather than the
-- duplicate alias. Closes #81194.
-- The guard is on the analyzer path only, so this SET is load-bearing: without it the old
-- analyzer resolves the expression and reports the alias collision instead. A session SET
-- also holds under the stress `compatibility` randomization, which a tag would not.
SET enable_analyzer = 1;

SELECT 'matcher in default expression';
CREATE TABLE t_matcher_default (c0 Int DEFAULT tuple(1 AS a0, * + 2 AS a0)) ENGINE = Memory; -- { serverError UNKNOWN_IDENTIFIER }

-- A matcher-free default must still be accepted, so that a change rejecting every default
-- expression reddens differently from the guard firing.
SELECT 'no matcher';
CREATE TABLE t_no_matcher (a Int, c0 Int DEFAULT a + 2) ENGINE = Memory;
