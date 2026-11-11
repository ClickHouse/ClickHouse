-- The `partition` slot of `CHECK TABLE` / `OPTIMIZE TABLE` is parser-produced as an `ASTPartition`,
-- and the interpreters downcast it via `as<ASTPartition>()` (raising `LOGICAL_ERROR` otherwise), so
-- `readJSON` must reject any other node type at the deserialization boundary.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CHECK TABLE t PARTITION 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CHECK TABLE db.t PARTITION ID ''foo'''));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t PARTITION 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE db.t PARTITION ID ''foo'' FINAL'));

-- A non-`ASTPartition` node in the `partition` slot is parser-impossible: it would build a
-- `CHECK`/`OPTIMIZE` AST whose interpreter downcast (`ast.partition->as<ASTPartition>()`) hits an
-- internal error instead of a user-facing parse error. Reject it as `BAD_ARGUMENTS`.
SELECT formatQueryFromJSON('{"type":"CheckTableQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Identifier","name":"p"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"OptimizeQuery","table":{"type":"Identifier","name":"t"},"partition":{"type":"Identifier","name":"p"}}'); -- { serverError BAD_ARGUMENTS }
