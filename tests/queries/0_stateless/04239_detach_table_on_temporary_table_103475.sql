-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103475
--
-- PR #95905 narrowed the `DETACH` guard in `executeToTableImpl` to
-- `query.kind == Detach && query.isTemporary()`, which only catches explicit
-- `DETACH TEMPORARY TABLE` syntax. The unqualified form `DETACH TABLE tmp`
-- (where `tmp` resolves to a temporary table via `Context::ResolveExternal`)
-- silently no-op'd (it set an unused `is_detached` flag inside
-- `executeToTemporaryTable`). It must throw `SYNTAX_ERROR` instead, matching
-- the behavior of explicit `DETACH TEMPORARY TABLE`.

CREATE TEMPORARY TABLE t_103475 (x UInt32) ENGINE = Memory;
INSERT INTO t_103475 VALUES (1), (2), (3);

SELECT count() FROM t_103475;

-- Explicit `DETACH TEMPORARY TABLE` syntax: already throws (covered by 03701, kept here for symmetry).
DETACH TEMPORARY TABLE t_103475; -- { serverError SYNTAX_ERROR }

-- Unqualified `DETACH TABLE` on a temporary table: regression — must also throw.
DETACH TABLE t_103475; -- { serverError SYNTAX_ERROR }

-- Both failed attempts must leave the table accessible.
SELECT count() FROM t_103475;

-- `DETACH TEMPORARY TABLE IF EXISTS` on a non-existent name: must also throw.
-- This path never reaches `executeToTemporaryTable` because `Context::ResolveExternal` fails,
-- so the broad guard in `executeToTemporaryTable` alone would not cover it — without the
-- narrow guard in `executeToTableImpl`, the `if (query.if_exists) return {};` short-circuit
-- would silently no-op the (unsupported) operation.
DETACH TEMPORARY TABLE IF EXISTS no_such_t_103475; -- { serverError SYNTAX_ERROR }

DROP TEMPORARY TABLE t_103475;
