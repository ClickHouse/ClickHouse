-- Regression test for the AST JSON review finding on hidden UUID state in nested table references.
--
-- A `UUID` that execution honours must also be formattable back to SQL, otherwise
-- `formatQueryFromJSON` prints a name-only reference while the JSON AST resolves the table by `UUID`.
-- For an `ASTTableIdentifier` that holds when `has_uuid` marks a user-written clause, which
-- `formatImplWithoutAlias` emits; a `uuid` without it comes from internal rewriting and is not
-- emitted, so it must fail closed. `ASTViewTargets::formatTarget` prints only `db.table` for a
-- `table_id` target (only `inner_uuid` is emitted, as `INNER UUID '...'`), so its `table_uuid` is
-- rejected unconditionally.

-- ---------------------------------------------------------------------------
-- Valid shapes must keep round-tripping.
-- ---------------------------------------------------------------------------
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));

-- A `uuid` paired with `has_uuid` is a user-written clause and round-trips.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'), '"type":"TableIdentifier","name":"src"', '"type":"TableIdentifier","name":"src","has_uuid":true,"uuid":"a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5"'));

-- ---------------------------------------------------------------------------
-- Malformed JSON that hides a UUID behind a nested table reference must be rejected.
-- ---------------------------------------------------------------------------

-- A `uuid` injected onto a `REFRESH DEPENDS ON` dependency (`ASTTableIdentifier`) without `has_uuid`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH DEPENDS ON src ENGINE = Memory AS SELECT 1'), '"type":"TableIdentifier","name":"src"', '"type":"TableIdentifier","name":"src","uuid":"a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5"')); -- { serverError BAD_ARGUMENTS }

-- A `table_uuid` injected onto a materialized-view `TO` target (`ASTViewTargets` `table_id`).
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'), '"table_name":"dst"', '"table_name":"dst","table_uuid":"a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5"')); -- { serverError BAD_ARGUMENTS }
