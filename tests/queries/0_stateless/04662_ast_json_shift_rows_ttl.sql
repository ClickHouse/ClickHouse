-- Regression test for the AST JSON round-trip of the internal `MATERIALIZE TTL <delta>` form.
--
-- The fast `MODIFY TTL` optimization persists its mutation as `MATERIALIZE TTL <delta>`, an
-- internal-only command that `InterpreterAlterQuery` rejects when a user issues it directly.
-- `ASTAlterCommand::writeJSON` / `readJSON` must carry `ttl_delta`, otherwise the JSON AST path
-- silently rewrites the command into a plain `MATERIALIZE TTL`, i.e. turns a rejected internal form
-- into a real data-rewriting mutation.

-- The delta survives the round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL 100'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL -8640000'));

-- A plain `MATERIALIZE TTL` keeps round-tripping without a delta.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL IN PARTITION 1'));

-- The delta is still rejected on execution after the round-trip, exactly as when parsed from SQL.
CREATE TABLE t_ast_json_ttl_delta (d DateTime, k UInt64) ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 DAY;
ALTER TABLE t_ast_json_ttl_delta MATERIALIZE TTL 100; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ast_json_ttl_delta;

-- `ttl_delta` is meaningless for any other command type, so a JSON AST carrying it on a different
-- command must be rejected rather than silently ignored.
SELECT formatQueryFromJSON(replaceOne(parseQueryToJSON('ALTER TABLE t MATERIALIZE TTL 100'), '"MATERIALIZE_TTL"', '"REMOVE_TTL"')); -- { serverError BAD_ARGUMENTS }
