-- Extended tests for AST formatting round-trip correctness when
-- CREATE TABLE / CREATE VIEW / CREATE MATERIALIZED VIEW ... AS SELECT
-- appears inside an outer query that has trailing output options
-- (SETTINGS, FORMAT, INTO OUTFILE).
--
-- Companion to 04041, covering scenarios not exercised there:
--   1. All three EXPLAIN subtypes (AST, PLAN, PIPELINE) wrapping CREATE TABLE AS SELECT
--   2. CREATE VIEW AS SELECT with trailing output options
--   3. CREATE MATERIALIZED VIEW AS SELECT with trailing output options
--   4. INTO OUTFILE as the trailing output option (all three create forms)
--   5. INTERSECT / EXCEPT set operations in the AS-select
--   6. CREATE TABLE with its own SETTINGS *and* an outer FORMAT (dual-flag case)
--   7. No spurious parentheses: EXPLAIN SELECT with FORMAT / INTO OUTFILE stays clean
--   8. No spurious parentheses: CREATE TABLE AS SELECT without any output options


-- ============================================================
-- Section 1: EXPLAIN subtypes wrapping CREATE TABLE AS SELECT
-- ============================================================

-- 1a. EXPLAIN AST + SETTINGS
SELECT formatQuery('EXPLAIN AST CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1');
SELECT formatQuery('EXPLAIN AST CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
    = formatQuery(formatQuery('EXPLAIN AST CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));

-- 1b. EXPLAIN PLAN + FORMAT
SELECT formatQuery('EXPLAIN PLAN CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');
SELECT formatQuery('EXPLAIN PLAN CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('EXPLAIN PLAN CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));

-- 1c. EXPLAIN PIPELINE + SETTINGS
SELECT formatQuery('EXPLAIN PIPELINE CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1');
SELECT formatQuery('EXPLAIN PIPELINE CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
    = formatQuery(formatQuery('EXPLAIN PIPELINE CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));


-- ============================================================
-- Section 2: CREATE VIEW AS SELECT with trailing output options
-- ============================================================
-- Note: EXPLAIN SYNTAX does not support CREATE VIEW, so we test
-- bare CREATE VIEW with formatQuery instead.

-- 2a. CREATE VIEW + SETTINGS
SELECT formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1');
SELECT formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
    = formatQuery(formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));

-- 2b. CREATE VIEW + FORMAT
SELECT formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');
SELECT formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));

-- 2c. CREATE VIEW + INTO OUTFILE
SELECT formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null''');
SELECT formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null''')
    = formatQuery(formatQuery('CREATE VIEW v1 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null'''));


-- ============================================================
-- Section 3: CREATE MATERIALIZED VIEW AS SELECT
-- ============================================================
-- Note: EXPLAIN SYNTAX does not support CREATE MATERIALIZED VIEW,
-- so we test bare CREATE MATERIALIZED VIEW with formatQuery instead.

-- 3a. CREATE MATERIALIZED VIEW + SETTINGS
SELECT formatQuery('CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree() ORDER BY 1 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1');
SELECT formatQuery('CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree() ORDER BY 1 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
    = formatQuery(formatQuery('CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree() ORDER BY 1 AS SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));

-- 3b. CREATE MATERIALIZED VIEW + FORMAT
SELECT formatQuery('CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree() ORDER BY 1 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');
SELECT formatQuery('CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree() ORDER BY 1 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('CREATE MATERIALIZED VIEW mv1 ENGINE = MergeTree() ORDER BY 1 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));


-- ============================================================
-- Section 4: INTO OUTFILE as the trailing output option
-- ============================================================

-- 4a. CREATE TABLE AS SELECT + INTO OUTFILE (bare, no outer EXPLAIN)
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null''');
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null''')
    = formatQuery(formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null'''));

-- 4b. EXPLAIN SYNTAX + CREATE TABLE AS SELECT + INTO OUTFILE
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null''');
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null''')
    = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2 INTO OUTFILE ''/dev/null'''));


-- ============================================================
-- Section 5: INTERSECT / EXCEPT in the AS-select
-- ============================================================

-- 5a. INTERSECT in AS-select + outer SETTINGS
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 INTERSECT SELECT 1 SETTINGS max_threads = 1');
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 INTERSECT SELECT 1 SETTINGS max_threads = 1')
    = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 INTERSECT SELECT 1 SETTINGS max_threads = 1'));

-- 5b. EXCEPT in AS-select + outer FORMAT
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 EXCEPT SELECT 2 FORMAT JSON');
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 EXCEPT SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 EXCEPT SELECT 2 FORMAT JSON'));

-- 5c. INTERSECT without outer output options: no parens
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 INTERSECT SELECT 1');


-- ============================================================
-- Section 6: Dual-flag — CREATE TABLE with own SETTINGS + outer FORMAT
-- ============================================================

-- Both settings_ast (engine SETTINGS) and has_trailing_output_options (FORMAT) are set.
-- The formatter must parenthesize exactly once and not produce double parens.
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));

-- And the same combination under EXPLAIN
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON');
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 SETTINGS index_granularity = 8192 AS SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));


-- ============================================================
-- Section 7: No spurious parentheses — plain SELECT inside EXPLAIN
-- ============================================================

-- 7a. EXPLAIN SYNTAX SELECT + FORMAT: SELECT must NOT be wrapped in parens
SELECT formatQuery('EXPLAIN SYNTAX SELECT 1 UNION ALL SELECT 2 FORMAT JSON');
SELECT formatQuery('EXPLAIN SYNTAX SELECT 1 UNION ALL SELECT 2 FORMAT JSON')
    = formatQuery(formatQuery('EXPLAIN SYNTAX SELECT 1 UNION ALL SELECT 2 FORMAT JSON'));

-- 7b. EXPLAIN AST SELECT + SETTINGS: SELECT must NOT be wrapped in parens
SELECT formatQuery('EXPLAIN AST SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1');
SELECT formatQuery('EXPLAIN AST SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1')
    = formatQuery(formatQuery('EXPLAIN AST SELECT 1 UNION ALL SELECT 2 SETTINGS max_threads = 1'));

-- 7c. EXPLAIN SYNTAX SELECT + INTO OUTFILE: SELECT must NOT be wrapped in parens
SELECT formatQuery('EXPLAIN SYNTAX SELECT 1 INTO OUTFILE ''/dev/null''');
SELECT formatQuery('EXPLAIN SYNTAX SELECT 1 INTO OUTFILE ''/dev/null''')
    = formatQuery(formatQuery('EXPLAIN SYNTAX SELECT 1 INTO OUTFILE ''/dev/null'''));


-- ============================================================
-- Section 8: No spurious parentheses — CREATE TABLE without output options
-- ============================================================

-- 8a. No outer EXPLAIN, no trailing options: bare AS-select, no parens
SELECT formatQuery('CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2');

-- 8b. EXPLAIN with no output options: no parens needed either
SELECT formatQuery('EXPLAIN SYNTAX CREATE TABLE t1 (c0 Int) ENGINE = MergeTree() ORDER BY c0 AS SELECT 1 UNION ALL SELECT 2');
