-- The `partition` slot of `DELETE` is parser-produced as an `ASTPartition` (`InterpreterDeleteQuery`
-- splices `partition->formatWithSecretsOneLine()` into a synthesized `UPDATE`/`ALTER ... IN PARTITION`
-- query), and view/table-expression column-alias lists are parser-produced as `ASTExpressionList`s of
-- `ASTIdentifier` (`ParserAliasesExpressionList`) later downcast via `as<ASTIdentifier &>()`, so
-- `readJSON` must reject other node shapes at the deserialization boundary.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM t IN PARTITION ID ''p1'' WHERE x = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM db.t IN PARTITION 2 WHERE 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE VIEW v (a, b) AS SELECT 1, 2'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b FROM (SELECT 1, 2) AS t(a, b)'));

-- A non-`ASTPartition` node in the DELETE `partition` slot is parser-impossible: formatting it into the
-- synthesized `IN PARTITION` clause could change the partition semantics or fail late at reparse.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('DELETE FROM t IN PARTITION ID ''p1'' WHERE x = 1'),
    '"type":"Partition"', '"type":"Identifier","name":"p"')); -- { serverError BAD_ARGUMENTS }

-- Non-identifier children in the CREATE view aliases list would reach the internal
-- `as<ASTIdentifier &>()` cast in `InterpreterCreateQuery`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE VIEW v (a, b) AS SELECT 1, 2'),
    '{"type":"Identifier","name":"b"}', '{"type":"Literal","value":{"field_type":"UInt64","value":42}}')); -- { serverError BAD_ARGUMENTS }

-- Non-identifier children in a table-expression column-alias list would reach the internal
-- `as<ASTIdentifier &>()` cast in `QueryTreeBuilder`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('SELECT a, b FROM (SELECT 1, 2) AS t(a, b)'),
    '"column_aliases":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}',
    '"column_aliases":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":42}}')); -- { serverError BAD_ARGUMENTS }
