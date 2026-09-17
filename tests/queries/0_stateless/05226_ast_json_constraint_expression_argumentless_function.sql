-- A constraint expression is consumed before analysis: `ConstraintsDescription::update` hands it to
-- `TreeCNFConverter` and to `ComparisonGraph`, both of which dereference `ASTFunction::arguments`
-- without checking it. Inside an expression the parser always fills that list, because it builds a
-- function only after an opening parenthesis; outside one it does not, which is why a table engine, a
-- `CODEC`/`STATISTICS` element and an index `TYPE` are argument-less functions and `ASTFunction::readJSON`
-- cannot require an `"arguments"` member on its own. The constraint slot therefore rejects the
-- parser-impossible node itself.

-- Parser-produced shapes stay accepted; the reference pins what they format back to.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8 CODEC(LZ4), b UInt8 STATISTICS(tdigest), INDEX i a TYPE minmax GRANULARITY 1, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a IN (SELECT 1)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t (MODIFY CONSTRAINT c CHECK a > 0 AND a < 9)'));

-- `ASTColumnsApplyTransformer` owns `lambda` and `parameters` outside `IAST::children`, which is what the
-- rejection walks, so these two shapes lie outside it; pin that they keep round-tripping unchanged.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK COLUMNS(''a'') APPLY (x -> x > 0)) ENGINE = MergeTree ORDER BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK COLUMNS(''a'') APPLY quantile(0.5)) ENGINE = MergeTree ORDER BY a'));

-- A present but empty argument list is a shape the parser does produce (`greater()`), and the
-- optimizers already tolerate it, so it must keep round-tripping and stay a run-time error only.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK greater()) ENGINE = MergeTree ORDER BY a'));

-- Each deletion below must leave a parseable document, otherwise the negatives would fail with
-- `BAD_ARGUMENTS` raised by the JSON parser instead of by the check under test.
SELECT
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK NOT a) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}', '')),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8, CONSTRAINT c CHECK a > 0 AND b > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"b"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')),
    isValidJSON(replace(parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c ASSUME a > 0) ENGINE = MergeTree ORDER BY a'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')),
    isValidJSON(replace(parseQueryToJSON('ALTER TABLE t (ADD CONSTRAINT c CHECK a > 0)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', '')),
    isValidJSON(replace(parseQueryToJSON('ALTER TABLE t (MODIFY CONSTRAINT c CHECK a > 0)'), ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}', ''));

-- A comparison without an argument list is what `ComparisonGraph`'s `getArguments` dereferences.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK a > 0) ENGINE = MergeTree ORDER BY a'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- A logical function without an argument list is what `TreeCNFConverter::splitMultiLogic` dereferences.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c CHECK NOT a) ENGINE = MergeTree ORDER BY a'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- The node is rejected anywhere in the expression, not only at its root.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt8, b UInt8, CONSTRAINT c CHECK a > 0 AND b > 0) ENGINE = MergeTree ORDER BY a'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"b"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- An `ASSUME` constraint reaches the same consumers as a `CHECK` one.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (a UInt8, CONSTRAINT c ASSUME a > 0) ENGINE = MergeTree ORDER BY a'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}',
    '')); -- { serverError BAD_ARGUMENTS }

-- `ALTER TABLE ... ADD/MODIFY CONSTRAINT` reads the same declaration node.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('ALTER TABLE t (ADD CONSTRAINT c CHECK a > 0)'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}',
    '')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('ALTER TABLE t (MODIFY CONSTRAINT c CHECK a > 0)'),
    ',"arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Literal","value":{"field_type":"UInt64","value":0}}]}',
    '')); -- { serverError BAD_ARGUMENTS }
