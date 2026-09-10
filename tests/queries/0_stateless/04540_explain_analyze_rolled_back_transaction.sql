-- Tags: no-ordinary-database
-- Regression test: EXPLAIN ANALYZE executes the inner SELECT, so once a transaction has failed
-- (ROLLED_BACK) it must be rejected like a plain SELECT and not sneak through the
-- "special query" exemption for EXPLAIN. Non-executing EXPLAIN kinds keep working.

DROP TABLE IF EXISTS t_04540;
CREATE TABLE t_04540 (n Int64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_04540 VALUES (1);

BEGIN TRANSACTION;
-- Force the transaction into the ROLLED_BACK state.
SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- A plain SELECT is rejected in a failed transaction.
SELECT n FROM t_04540; -- { serverError INVALID_TRANSACTION }

-- EXPLAIN ANALYZE executes the inner SELECT, so it must be rejected as well.
EXPLAIN ANALYZE SELECT n FROM t_04540; -- { serverError INVALID_TRANSACTION }

-- Non-executing EXPLAIN forms only inspect the query and stay allowed.
-- Use EXPLAIN AST: it dumps the parsed AST before analysis, so its output is
-- identical under both analyzers (unlike EXPLAIN SYNTAX, whose normalized query
-- differs, e.g. the implicit `FROM system.one` is only added by the analyzer).
EXPLAIN AST SELECT 1;

ROLLBACK;

DROP TABLE t_04540;
