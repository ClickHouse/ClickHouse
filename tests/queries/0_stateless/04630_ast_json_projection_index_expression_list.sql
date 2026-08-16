-- The `index` slot of a projection declaration is parser-produced only as a non-empty
-- `ASTExpressionList` (`ParserProjectionDeclaration` uses `ParserNotEmptyExpressionList`). With
-- `TYPE commit_order`, `ProjectionIndexCommitOrder::fillProjectionDescription` clones `index` into
-- the projection SELECT slot, and `ASTProjectionSelectQuery::cloneToASTSelect` raises an internal
-- error unless that slot is an `ASTExpressionList`, so `readJSON` must reject any other shape at
-- the deserialization boundary.

-- Parser-produced shapes round-trip byte-identically.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p INDEX a TYPE commit_order) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, b UInt64, PROJECTION p INDEX a, b TYPE commit_order) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p (SELECT a ORDER BY a)) ENGINE = MergeTree ORDER BY a'));

-- A non-`ASTExpressionList` node in the `index` slot is parser-impossible and would only fail later
-- inside projection-description construction as an internal AST-structure error. Reject it as
-- `BAD_ARGUMENTS`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p INDEX a TYPE commit_order) ENGINE = MergeTree ORDER BY a'),
    '"index":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}',
    '"index":{"type":"Literal","value":{"field_type":"UInt64","value":1}}')); -- { serverError BAD_ARGUMENTS }

-- An empty expression list in the `index` slot is equally parser-impossible
-- (`ParserNotEmptyExpressionList`) and would format as `INDEX  TYPE ...`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt64, PROJECTION p INDEX a TYPE commit_order) ENGINE = MergeTree ORDER BY a'),
    '"index":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}',
    '"index":{"type":"ExpressionList","children":[]}')); -- { serverError BAD_ARGUMENTS }
