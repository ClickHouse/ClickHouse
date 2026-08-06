-- Verify that CREATE TABLE ... AS SELECT round-trips correctly when trailing
-- output options (FORMAT, SETTINGS) are present on an outer query like EXPLAIN.
-- The formatter must parenthesize the AS-select so that the output options are
-- not consumed by the inner SELECT during re-parsing.

-- EXPLAIN CREATE TABLE ... AS (SELECT ...) SETTINGS: parentheses preserved
-- (with the original AS-select in parens, SETTINGS stays on the EXPLAIN)
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS (SELECT 1 UNION ALL SELECT 2) SETTINGS max_threads = 1');

-- Round-trip: formatting twice must produce the same result
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS (SELECT 1 UNION ALL SELECT 2) SETTINGS max_threads = 1')
     = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS (SELECT 1 UNION ALL SELECT 2) SETTINGS max_threads = 1'));

-- EXPLAIN CREATE TABLE ... AS SELECT ... FORMAT: parentheses required
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');

-- Round-trip for FORMAT case
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
     = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));

-- Without output options: no parentheses
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2');

-- CREATE TABLE with its own SETTINGS and output SETTINGS
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
     = formatQuery(formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));

-- EXPLAIN SELECT with FORMAT: no parentheses around the SELECT
SELECT formatQuery('EXPLAIN SYNTAX SELECT 1 FORMAT TSVRaw');

-- Round-trip for EXPLAIN SELECT with FORMAT
SELECT formatQuery('EXPLAIN SYNTAX SELECT 1 FORMAT TSVRaw')
     = formatQuery(formatQuery('EXPLAIN SYNTAX SELECT 1 FORMAT TSVRaw'));
