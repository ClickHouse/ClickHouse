-- Regression test for the write-side of the AST JSON nested-UUID review finding.
--
-- `ParserRefreshStrategy` accepts `REFRESH DEPENDS ON src UUID '...'` and `ParserViewTargets` accepts a
-- `TO dst UUID '...'` target, so the parser can produce an `ASTTableIdentifier` / `ASTViewTargets` `table_id`
-- that carries a `UUID`. `writeJSON` may emit such a `UUID` only where the SQL formatter emits it back:
-- `ASTTableIdentifier` formats a user-written clause (`has_uuid`), so it serializes both fields, while an
-- `ASTViewTargets` `table_uuid` is never formatted and must fail closed, so that `parseQueryToJSON` rejects
-- the unsupported shape instead of emitting JSON `formatQueryFromJSON` / `clickhouse_json` cannot read back.

-- ---------------------------------------------------------------------------
-- A `UUID` clause the SQL formatter emits is serialized and reads back.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src UUID \'a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5\' ENGINE = Memory AS SELECT 1'));

-- ---------------------------------------------------------------------------
-- A nested UUID no formatter emits must be rejected during serialization.
-- ---------------------------------------------------------------------------

-- A `UUID` on a materialized-view `TO` target (`ASTViewTargets` `table_id`).
SELECT parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst UUID \'a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5\' AS SELECT 1'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- The same references without a UUID must keep round-tripping.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));
