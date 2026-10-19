-- A constraint expression is consumed before analysis: `ConstraintsDescription::update` hands it to
-- `TreeCNFConverter` and to `ComparisonGraph`, both of which dereference `ASTFunction::arguments` without
-- checking it. The parser fills that list inside an expression and leaves it absent outside one, which is
-- why a table engine, a `CODEC`/`STATISTICS` element and an index `TYPE` are argument-less functions and
-- `ASTFunction::readJSON` cannot require the member on its own. The constraint slot therefore rejects the
-- parser-impossible node itself.

-- Accepted shapes: a plain and a subquery constraint, the argument-less functions an over-broad screen
-- would break, and `greater()`, whose argument list is present but empty.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8 CODEC(LZ4), b UInt8 STATISTICS(tdigest), INDEX i a TYPE minmax GRANULARITY 1, CONSTRAINT c CHECK a > 0, CONSTRAINT d CHECK a IN (SELECT 1), CONSTRAINT e CHECK greater()) ENGINE = MergeTree ORDER BY a'));
-- `ASTColumnsApplyTransformer` owns `lambda` and `parameters` outside `IAST::children`, which is what the
-- rejection walks, so these two shapes lie outside it and must keep round-tripping unchanged.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK COLUMNS(''a'') APPLY (x -> x > 0), CONSTRAINT d CHECK COLUMNS(''a'') APPLY quantile(0.5)) ENGINE = MergeTree ORDER BY a'));

-- Each deletion below must leave a parseable document holding exactly one copy of the member, otherwise a
-- negative could pass because the JSON parser rejected the payload instead of the check under test.
SELECT
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}') = 1 AND isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK NOT a) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}') = 1 AND isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK NOT a) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}', '')),
    countSubstrings(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8, CONSTRAINT c CHECK a > 0 AND b > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"b"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}') = 1 AND isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8, CONSTRAINT c CHECK a > 0 AND b > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"b"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')),
    countSubstrings(parseQueryToJSON('ALTER TABLE t (ADD CONSTRAINT c CHECK a > 0)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}') = 1 AND isValidJSON(replace(parseQueryToJSON('ALTER TABLE t (ADD CONSTRAINT c CHECK a > 0)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', ''));

-- A comparison, whose argument list `ComparisonGraph::getArguments` dereferences.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')); -- { serverError BAD_ARGUMENTS }
-- A logical function, whose argument list `TreeCNFConverter::splitMultiLogic` dereferences.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK NOT a) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}', '')); -- { serverError BAD_ARGUMENTS }
-- The node is rejected anywhere in the expression, not only at its root.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8, CONSTRAINT c CHECK a > 0 AND b > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"b"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')); -- { serverError BAD_ARGUMENTS }
-- `ALTER TABLE ... ADD/MODIFY CONSTRAINT` reads the same declaration node through the same reader.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t (ADD CONSTRAINT c CHECK a > 0)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')); -- { serverError BAD_ARGUMENTS }
