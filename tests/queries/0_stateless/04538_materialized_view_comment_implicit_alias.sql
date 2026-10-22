-- Tags: no-parallel

DROP TABLE IF EXISTS src_04538;
DROP TABLE IF EXISTS dst_04538;
DROP TABLE IF EXISTS mv_04538;
DROP TABLE IF EXISTS mv_04538_dup;

CREATE TABLE src_04538 (x UInt8) ENGINE = Memory;
CREATE TABLE dst_04538 (x UInt8) ENGINE = Memory;

-- Bug: previously failed to parse because COMMENT was consumed as an implicit alias for src_04538
CREATE MATERIALIZED VIEW mv_04538 TO dst_04538 AS SELECT x FROM src_04538 COMMENT 'trailing comment test';

SELECT comment FROM system.tables WHERE name = 'mv_04538' AND database = currentDatabase();

DROP TABLE mv_04538;

-- Explicit alias form should still work
CREATE MATERIALIZED VIEW mv_04538 TO dst_04538 AS SELECT x FROM src_04538 AS s COMMENT 'explicit alias comment';

SELECT comment FROM system.tables WHERE name = 'mv_04538' AND database = currentDatabase();

DROP TABLE mv_04538;

-- Explicit AS alias with the word "comment" should still work (not affected by restricted_keywords fix)
SELECT 1 AS comment;

-- Regression test: comment specified before AS SELECT should not prevent
-- an implicit alias named "comment" from being parsed correctly inside
-- the SELECT body itself (bot-flagged scenario).
CREATE VIEW v_04538 COMMENT 'view comment' AS SELECT 1 comment;

SELECT comment FROM system.tables WHERE name = 'v_04538' AND database = currentDatabase();
SELECT comment FROM v_04538;

DROP TABLE v_04538;

-- Regression test: implicit alias "comment" inside a materialized view AS SELECT
-- body, when the view itself has no comment at all, should still work
-- (bot-flagged scenario).
CREATE MATERIALIZED VIEW mv_04538 TO dst_04538 AS SELECT x FROM src_04538 comment;

SELECT x FROM dst_04538;

DROP TABLE mv_04538;

DROP TABLE src_04538;
DROP TABLE dst_04538;

CREATE MATERIALIZED VIEW mv_04538_dup (x UInt8) ENGINE = Memory COMMENT 'pre-as comment' AS SELECT x FROM src_04538 COMMENT 'post-select comment'; -- { clientError SYNTAX_ERROR }
-- Regression test: implicit alias "comment" (without AS) must still work outside
-- of a materialized view's AS SELECT body, since the restricted-keyword check
-- for COMMENT is scoped only to that context.
SELECT 1 comment;
SELECT number FROM numbers(1) comment;

-- Regression test: same duplicate-comment check must also apply to
-- ParserCreateWindowViewQuery (window views use a separate parser from
-- regular views), giving a clear SYNTAX_ERROR instead of a generic one
-- (bot-flagged scenario).
SET allow_experimental_window_view = 1;
SET allow_experimental_analyzer = 0;

DROP TABLE IF EXISTS src_wv_04538;
CREATE TABLE src_wv_04538 (x UInt8, ts DateTime) ENGINE = Memory;

CREATE WINDOW VIEW wv_04538_dup (x UInt8, w_start DateTime) ENGINE = Memory COMMENT 'pre-as comment' AS SELECT x, tumbleStart(w_id) AS w_start FROM src_wv_04538 GROUP BY x, tumble(ts, toIntervalSecond(5)) AS w_id COMMENT 'post-select comment'; -- { clientError SYNTAX_ERROR }

-- Single comment (before AS SELECT) on a window view should still work.
CREATE WINDOW VIEW wv_04538_single (x UInt8, w_start DateTime) ENGINE = Memory COMMENT 'single wv comment' AS SELECT x, tumbleStart(w_id) AS w_start FROM src_wv_04538 GROUP BY x, tumble(ts, toIntervalSecond(5)) AS w_id;

SELECT comment FROM system.tables WHERE name = 'wv_04538_single' AND database = currentDatabase();

DROP TABLE wv_04538_single;
DROP TABLE src_wv_04538;

-- Regression test: a heredoc-style comment literal ($tag$...$tag$) must also be
-- recognized by the trailing-comment lookahead, not just ordinary quoted string
-- literals, since parseComment() accepts both forms (bot-flagged scenario).
DROP TABLE IF EXISTS heredoc_src_04538;
DROP TABLE IF EXISTS heredoc_mv_04538;
CREATE TABLE heredoc_src_04538 (x UInt8) ENGINE = Memory;
CREATE MATERIALIZED VIEW heredoc_mv_04538 ENGINE = MergeTree ORDER BY tuple() AS SELECT x FROM heredoc_src_04538 COMMENT $heredoc$heredoc trailing comment$heredoc$;

SELECT comment FROM system.tables WHERE name = 'heredoc_mv_04538' AND database = currentDatabase();

DROP TABLE heredoc_mv_04538;
DROP TABLE heredoc_src_04538;

-- Regression test: same duplicate-comment check must also apply to the
-- CREATE TABLE ... AS SELECT form (ParserCreateTableQuery), since the
-- COMMENT-as-implicit-alias lookahead fix is global to ParserAlias and
-- also affects table AS SELECT parsing (bot-flagged scenario).
DROP TABLE IF EXISTS src_tbl_04538;
CREATE TABLE src_tbl_04538 (x UInt8) ENGINE = Memory;

CREATE TABLE t_dup_04538 ENGINE = Memory COMMENT 'pre-as comment' AS SELECT x FROM src_tbl_04538 COMMENT 'post-select comment'; -- { clientError SYNTAX_ERROR }

-- Single comment (before AS SELECT) on a plain table should still work.
CREATE TABLE t_single_04538 ENGINE = Memory COMMENT 'single as-select comment' AS SELECT x FROM src_tbl_04538;

SELECT comment FROM system.tables WHERE name = 't_single_04538' AND database = currentDatabase();

DROP TABLE t_single_04538;

-- A plain CREATE TABLE (no AS SELECT at all) should be unaffected.
CREATE TABLE t_plain_04538 (x UInt8) ENGINE = Memory COMMENT 'plain table comment';

SELECT comment FROM system.tables WHERE name = 't_plain_04538' AND database = currentDatabase();

DROP TABLE t_plain_04538;
DROP TABLE src_tbl_04538;

-- Regression test: the same duplicate-comment check must also cover the
-- AS table and AS table_function() forms of CREATE TABLE, not just AS SELECT,
-- since CREATE TABLE t COMMENT 'pre' AS base COMMENT 'post' was already
-- falling back to the old generic trailing-token error (bot-flagged scenario).
DROP TABLE IF EXISTS base_04538;
CREATE TABLE base_04538 (a Int32) ENGINE = TinyLog COMMENT 'original comment';

CREATE TABLE t_astable_dup_04538 COMMENT 'pre comment' AS base_04538 COMMENT 'post comment'; -- { clientError SYNTAX_ERROR }
CREATE TABLE t_astf_dup_04538 COMMENT 'pre comment' AS numbers(5) COMMENT 'post comment'; -- { clientError SYNTAX_ERROR }

-- Single comment (after AS, no pre-AS comment) on the AS table form should still work.
CREATE TABLE t_astable_single_04538 AS base_04538 COMMENT 'single as-table comment';

SELECT comment FROM system.tables WHERE name = 't_astable_single_04538' AND database = currentDatabase();

DROP TABLE t_astable_single_04538;
DROP TABLE base_04538;
