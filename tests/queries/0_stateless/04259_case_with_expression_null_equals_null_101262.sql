-- https://github.com/ClickHouse/ClickHouse/issues/101262
-- CASE semantics require NULL = NULL to be true. The `transform` optimization path
-- uses standard equality, so it must be skipped when WHEN values are Nullable.

SELECT caseWithExpression(materialize(NULL), NULL, 'YES', 'NO');
SELECT caseWithExpression(materialize(NULL), materialize(NULL), 'YES', 'NO');
SELECT caseWithExpression(materialize(CAST(NULL AS Nullable(Nothing))), CAST(NULL AS Nullable(Nothing)), 'YES', 'NO');

SELECT caseWithExpression(materialize(CAST(NULL AS Nullable(Int32))), CAST(NULL AS Nullable(Int32)), 'matched_null', 1, 'one', 'else');
SELECT caseWithExpression(materialize(toNullable(1)), CAST(NULL AS Nullable(Int32)), 'matched_null', 1, 'one', 'else');
SELECT caseWithExpression(materialize(toNullable(5)), CAST(NULL AS Nullable(Int32)), 'matched_null', 1, 'one', 'else');

SELECT CASE materialize(NULL) WHEN NULL THEN 'YES' ELSE 'NO' END;

SELECT caseWithExpression(1, 1, 'one', 2, 'two', 'other');
SELECT caseWithExpression(3, 1, 'one', 2, 'two', 'other');
