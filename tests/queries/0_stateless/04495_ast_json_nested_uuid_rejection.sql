-- Regression test for the AST JSON review finding on hidden UUID state in nested table references.
--
-- A `REFRESH DEPENDS ON src UUID '...'` dependency (an `ASTTableIdentifier`) and a materialized-view
-- `TO tgt` target (`ASTViewTargets` `table_id`) can carry a `UUID` that execution honours
-- (`ASTTableIdentifier::getTableId` / the target `StorageID`). But the SQL formatters for those nested
-- references never emit a `UUID` clause: `ASTIdentifier::formatImplWithoutAlias` prints only the name, and
-- `ASTViewTargets::formatTarget` prints only `db.table` for a `table_id` target (only `inner_uuid` is emitted,
-- as `INNER UUID '...'`). `formatQueryFromJSON` would therefore print a name-only reference while the JSON AST
-- still resolves the table by `UUID`. Such payloads must fail closed with `BAD_ARGUMENTS` at the boundary
-- instead of round-tripping to SQL that refers to a different table than the JSON AST executes against.

-- ---------------------------------------------------------------------------
-- Valid shapes without a nested UUID must keep round-tripping.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));

-- ---------------------------------------------------------------------------
-- Malformed JSON that hides a UUID behind a nested table reference must be rejected.
-- ---------------------------------------------------------------------------

-- A `uuid` injected onto a `REFRESH DEPENDS ON` dependency (`ASTTableIdentifier`).
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'), '"type":"TableIdentifier","name":"src"', '"type":"TableIdentifier","name":"src","uuid":"a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5"')); -- { serverError BAD_ARGUMENTS }

-- A `table_uuid` injected onto a materialized-view `TO` target (`ASTViewTargets` `table_id`).
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'), '"table_name":"dst"', '"table_name":"dst","table_uuid":"a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5"')); -- { serverError BAD_ARGUMENTS }
