-- `EXPLAIN AST optimize = 1` analyses the query with the interpreter that preceded the analyzer, which builds the
-- set of an `IN` subquery in `PREWHERE`. Building it over `remote` used to dereference a null query tree.

DROP TABLE IF EXISTS t_explain_prewhere_in_remote;
CREATE TABLE t_explain_prewhere_in_remote (x UInt64) ENGINE = MergeTree ORDER BY x;

-- `*` is resolved only when the query is analysed, which is where the set is built.
SELECT
    countIf(explain ILIKE '%Asterisk%') AS asterisk_left,
    countIf(explain ILIKE '%Function remote %') AS remote_kept
FROM (EXPLAIN AST optimize = 1 SELECT * FROM (SELECT x FROM t_explain_prewhere_in_remote PREWHERE x IN (SELECT x FROM remote('127.0.0.1', currentDatabase(), t_explain_prewhere_in_remote))));

DROP TABLE t_explain_prewhere_in_remote;
