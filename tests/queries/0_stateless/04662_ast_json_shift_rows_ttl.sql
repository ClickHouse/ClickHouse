-- Regression test for the AST JSON round-trip of the internal `SHIFT ROWS TTL BY <n> SECOND` form.
--
-- The fast `MODIFY TTL` optimization persists its mutation as `SHIFT ROWS TTL BY <n> SECOND`, an
-- internal-only command that `InterpreterAlterQuery` rejects when a user issues it directly.
-- `ASTAlterCommand::writeJSON` / `readJSON` must carry `ttl_shift`, otherwise the JSON AST path
-- silently drops the shift from the command.

-- The shift survives the round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t SHIFT ROWS TTL BY 100 SECOND'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t SHIFT ROWS TTL BY -8640000 SECOND'));

-- A plain `MATERIALIZE TTL` keeps round-tripping unchanged.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL IN PARTITION 1'));

-- The internal command is still rejected on execution after the round-trip, exactly as when parsed
-- from SQL.
CREATE TABLE t_ast_json_ttl_shift (d DateTime, k UInt64) ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 DAY;
ALTER TABLE t_ast_json_ttl_shift SHIFT ROWS TTL BY 100 SECOND; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ast_json_ttl_shift;

-- `ttl_shift` is meaningless for any other command type, so a JSON AST carrying it on a different
-- command must be rejected rather than silently ignored.
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('ALTER TABLE t SHIFT ROWS TTL BY 100 SECOND'), '"SHIFT_ROWS_TTL"', '"REMOVE_TTL"')); -- { serverError BAD_ARGUMENTS }
