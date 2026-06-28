-- The `limit`/`offset` settings are a global cap on the whole result. For a top-level UNION ALL whose
-- branches use LIMIT AFTER/UNTIL, the cap must be applied once over the merged union result, not once
-- per branch (which would let two branches return up to 2*limit rows). The settings are moved off the
-- range branch nodes onto the UnionNode and applied as a single outer step after the union.
--
-- UNION ALL does not define which branch is consumed first, so each branch emits the same constant value
-- and the test asserts the number of output rows: a single global cap yields `limit` rows, while a
-- per-branch cap would yield up to 2*limit. Offset/limit value semantics over a range are covered by
-- 04304_limit_after_until_settings_limit.

-- { echo }

-- Two range branches, settings limit = 2: the union result is capped to 2 rows total (not 4).
(SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) UNION ALL (SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 2;
(SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) UNION ALL (SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 2, enable_analyzer = 0;

-- limit + offset applied once after the union: 2 rows total, not 2 per branch.
(SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) UNION ALL (SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 2, offset = 1;
(SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) UNION ALL (SELECT 7 AS n FROM numbers(5) LIMIT AFTER number >= 0) SETTINGS limit = 2, offset = 1, enable_analyzer = 0;

-- UNTIL branches, settings limit once.
(SELECT 7 AS n FROM numbers(5) LIMIT UNTIL number >= 3) UNION ALL (SELECT 7 AS n FROM numbers(5) LIMIT UNTIL number >= 3) SETTINGS limit = 2;
(SELECT 7 AS n FROM numbers(5) LIMIT UNTIL number >= 3) UNION ALL (SELECT 7 AS n FROM numbers(5) LIMIT UNTIL number >= 3) SETTINGS limit = 2, enable_analyzer = 0;
