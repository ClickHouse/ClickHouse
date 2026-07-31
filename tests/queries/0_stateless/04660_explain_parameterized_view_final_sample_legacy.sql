-- The legacy formatting path (`ExplainAnalyzedSyntaxVisitor`, used by `EXPLAIN SYNTAX` with
-- `enable_analyzer = 0` and by `EXPLAIN AST optimize = 1`) must keep a parameterized view call
-- unexpanded when the table expression carries `FINAL` or `SAMPLE`. Those modifiers are valid on
-- the view call at execution time, but attaching them to a subquery produces a form the executor
-- rejects, so rewriting `pv(...) FINAL` into `(SELECT ...) FINAL` would make the `EXPLAIN` output
-- non-executable even though the real `SELECT` is valid.

DROP TABLE IF EXISTS t_04660;
DROP VIEW IF EXISTS pv_04660;

CREATE TABLE t_04660 (k UInt64, v String) ENGINE = ReplacingMergeTree ORDER BY (k, intHash32(k)) SAMPLE BY intHash32(k);
INSERT INTO t_04660 VALUES (1, 'a'), (2, 'b');

CREATE VIEW pv_04660 AS SELECT k, v FROM t_04660 WHERE k = {p:UInt64};

SET enable_analyzer = 0;

SELECT '-- real legacy SELECTs with FINAL / SAMPLE are valid';
SELECT * FROM pv_04660(p = 1) FINAL;
SELECT count() >= 0 FROM pv_04660(p = 1) SAMPLE 1/2;

SELECT '-- legacy EXPLAIN SYNTAX keeps the parameterized view call intact';
EXPLAIN SYNTAX SELECT * FROM pv_04660(p = 1) FINAL;
EXPLAIN SYNTAX SELECT * FROM pv_04660(p = 1) SAMPLE 1/2;

SELECT '-- EXPLAIN AST optimize = 1 keeps the parameterized view call intact';
EXPLAIN AST optimize = 1 SELECT * FROM pv_04660(p = 1) FINAL;
EXPLAIN AST optimize = 1 SELECT * FROM pv_04660(p = 1) SAMPLE 1/2;

SELECT '-- without FINAL / SAMPLE the view body is still expanded';
EXPLAIN SYNTAX SELECT * FROM pv_04660(p = 1);

SET enable_analyzer = 1;

SELECT '-- EXPLAIN AST optimize = 1 uses the same legacy visitor under the analyzer too';
EXPLAIN AST optimize = 1 SELECT * FROM pv_04660(p = 1) FINAL;

DROP VIEW pv_04660;
DROP TABLE t_04660;
