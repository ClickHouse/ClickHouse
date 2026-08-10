-- The `partition` slot of an ALTER command is not an arbitrary expression: the parser builds it
-- with `ParserPartition` for the `... PARTITION ...` forms and with a string literal or query
-- parameter for the `... PART ...` forms (the `part` flag, produced only for the
-- `DROP`/`DROP DETACHED`/`ATTACH`/`MOVE`/`FETCH` `PART` commands). Likewise, the `partition`
-- (`IN PARTITION`) slot of a lightweight `UPDATE` is always an `ASTPartition`. Execution
-- downcasts those shapes (`getPartitionIDFromQuery`, `getPartNameFromAST`), so `readJSON` must
-- reject any other node shape at the deserialization boundary.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP PARTITION 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP PART ''all_1_1_0'''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DETACH PART ''all_1_1_0'''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t ATTACH PART ''all_1_1_0'''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MOVE PART ''all_1_1_0'' TO DISK ''d1'''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t FETCH PART ''all_1_1_0'' FROM ''/zk/path'''));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t DROP PART {p:String}'));
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE t SET x = 1 IN PARTITION 1 WHERE x > 0'));

-- An `ASTPartition` in the `PART` slot is parser-impossible: it would format parser-impossible
-- SQL such as `DROP PART PARTITION 1` and bypass the `getPartNameFromAST` string requirement.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PARTITION 1'), '"part":false', '"part":true')); -- { serverError BAD_ARGUMENTS }

-- A bare literal in the `PARTITION` slot is parser-impossible: `getPartitionIDFromQuery`
-- downcasts it with `as<ASTPartition &>()`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PART ''all_1_1_0'''), '"part":true', '"part":false')); -- { serverError BAD_ARGUMENTS }

-- The parser produces `part` only for the `DROP`/`DROP DETACHED`/`ATTACH`/`MOVE`/`FETCH` `PART`
-- commands, never for `FREEZE`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t FREEZE PARTITION 1'), '"part":false', '"part":true')); -- { serverError BAD_ARGUMENTS }

-- A non-string literal in the `PART` slot is parser-impossible: `getPartNameFromAST` reads the
-- part name with `safeGet<String>`.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t DROP PART ''all_1_1_0'''), '{"field_type":"String","value":"all_1_1_0"}', '{"field_type":"UInt64","value":1}')); -- { serverError BAD_ARGUMENTS }

-- A non-`ASTPartition` node in the `UPDATE` `IN PARTITION` slot is parser-impossible:
-- `InterpreterUpdateQuery` forwards it into `ASTAlterCommand::partition`, where mutation
-- execution downcasts it with `as<ASTPartition &>()`.
SELECT formatQueryFromJSON('{"type":"UpdateQuery","table":{"type":"Identifier","name":"t"},"assignments":{"type":"ExpressionList","children":[{"type":"Assignment","column_name":"x","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}]},"partition":{"type":"Identifier","name":"p"},"predicate":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
