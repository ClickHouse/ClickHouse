-- A matcher in a column DEFAULT expression is now expanded against the table's columns instead of
-- being rejected up front. The only column here is `c0`, so `*` expands to `c0` and the default of
-- `c0` ends up referencing `c0`; the cycle is what gets reported, not the matcher.
-- Related: #81194.
-- The expansion happens during metadata normalization and therefore holds for both analyzers; the
-- session SET keeps the reported error stable under the stress `compatibility` randomization.
SET enable_analyzer = 1;

SELECT 'matcher in default expression';
CREATE TABLE t_matcher_default (c0 Int DEFAULT tuple(1 AS a0, * + 2 AS a0)) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }

-- A matcher-free default must still be accepted, so that a change rejecting every default
-- expression reddens differently from the cycle being reported.
SELECT 'no matcher';
CREATE TABLE t_no_matcher (a Int, c0 Int DEFAULT a + 2) ENGINE = Memory;
