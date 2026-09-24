-- `ASTSystemQuery::Type`, `ServerType::Type`, `RefreshScheduleKind` and `ASTAlterCommand::Type` each
-- carry an enumerator that names no value (an enumeration bound, or an unset default), and
-- `magic_enum::enum_cast` accepts those names like any other. No parse produces them, so the JSON
-- reader must reject them instead of building a tree that either has no rendering at all or formats
-- back to unparsable SQL.

SET enable_json_ast_dialect = 1;

-- Positives: the shapes the guards must not over-reject round-trip byte-identically.

SELECT formatQueryFromJSON(parseQueryToJSON($$SYSTEM START LISTEN TCP$$))
    = formatQuerySingleLine($$SYSTEM START LISTEN TCP$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SYSTEM START LISTEN QUERIES ALL EXCEPT TCP$$))
    = formatQuerySingleLine($$SYSTEM START LISTEN QUERIES ALL EXCEPT TCP$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY TO dst AS SELECT 1$$))
    = formatQuerySingleLine($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY TO dst AS SELECT 1$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH AFTER 1 HOUR TO dst AS SELECT 1$$))
    = formatQuerySingleLine($$CREATE MATERIALIZED VIEW mv REFRESH AFTER 1 HOUR TO dst AS SELECT 1$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH DEPENDS ON t TO dst AS SELECT 1$$))
    = formatQuerySingleLine($$CREATE MATERIALIZED VIEW mv REFRESH DEPENDS ON t TO dst AS SELECT 1$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$ALTER TABLE t DROP COLUMN c$$))
    = formatQuerySingleLine($$ALTER TABLE t DROP COLUMN c$$);

-- Negatives: one per enumerator that names no value. Without the guards the first two reach the
-- formatter's `Unknown SYSTEM command` arm, the next three format back to SQL the parser rejects,
-- and the last reaches `ASTAlterCommand::formatImpl`'s `Unexpected type of ALTER` arm.

SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"UNKNOWN"}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":"END"}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$SYSTEM START LISTEN TCP$$),
    '"server_type":{"type":"TCP"', '"server_type":{"type":"END"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$SYSTEM START LISTEN QUERIES ALL EXCEPT TCP$$),
    '"exclude_types":["TCP"]', '"exclude_types":["END"]')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY TO dst AS SELECT 1$$),
    '"schedule_kind":"EVERY"', '"schedule_kind":"UNKNOWN"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON('{"type":"AlterCommand","command_type":"NO_TYPE"}'); -- { serverError BAD_ARGUMENTS }
