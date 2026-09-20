-- In debug builds, the server checks AST formatting roundtrip consistency.
-- Previously, RoundBracketsLayer promoted ((tuple_literal)) to tuple(tuple_literal) unconditionally.
-- When the ON clause wrapped an aliased tuple expression with extra parens, e.g. ON ((1, 0.648) AS a7),
-- the reparse applied the promotion (because (1, 0.648) was parsed as ASTLiteral(Tuple) by
-- ParserCollectionOfLiterals), producing ON tuple((1, 0.648) AS a7), breaking the roundtrip.
-- The fix skips the promotion when the element has an alias.
-- Also fix position restoration in tryParseOperator when parseLambda fails after consuming '->'.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=a5e9d1c7da638871e8a4c99fda083c9d4dc9ffdc&name_0=MasterCI&name_1=BuzzHouse%20%28amd_debug%29

SELECT ((1), 0.648);
SELECT (((1), 0.648) AS a7);
SELECT [((1), 2), 3];
