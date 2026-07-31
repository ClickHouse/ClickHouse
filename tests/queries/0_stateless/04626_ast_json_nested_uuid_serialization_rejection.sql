-- Regression test for the write-side of the AST JSON nested-UUID review finding.
--
-- `ParserRefreshStrategy` accepts `REFRESH DEPENDS ON src UUID '...'` and `ParserViewTargets` accepts a
-- `TO dst UUID '...'` target, so the parser can produce an `ASTTableIdentifier` / `ASTViewTargets` `table_id`
-- that carries a `UUID`. Those UUIDs are honoured by execution (`getTableId` / the target `StorageID`) but are
-- never emitted by the SQL formatters, so a `UUID`-bearing nested reference cannot round-trip through AST JSON.
-- `readJSON` already rejects such payloads (see `04495_ast_json_nested_uuid_rejection`); `writeJSON` must fail
-- closed symmetrically so `parseQueryToJSON` rejects the unsupported shape instead of emitting JSON that
-- `formatQueryFromJSON` / `clickhouse_json` cannot read back.

-- ---------------------------------------------------------------------------
-- Nested UUIDs the parser produces must be rejected during serialization.
-- ---------------------------------------------------------------------------

-- A `UUID` on a `REFRESH DEPENDS ON` dependency (`ASTTableIdentifier`).
SELECT parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src UUID \'a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5\' ENGINE = Memory AS SELECT 1'); -- { serverError BAD_ARGUMENTS }

-- A `UUID` on a materialized-view `TO` target (`ASTViewTargets` `table_id`).
SELECT parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst UUID \'a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5\' AS SELECT 1'); -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- The same references without a UUID must keep round-tripping.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));
