-- Regression for an MSan-detected dangling reference in
-- `UserDefinedSQLFunctionVisitor::tryToReplaceFunction`.
-- The visitor used to construct a `QueryNormalizer::Data` with a `{}` temporary as
-- `source_columns_set`; because `Data` stores it by reference, the temporary died
-- before `QueryNormalizer::visit(ASTIdentifier&, ...)` reached
-- `data.source_columns_set.contains(node.name())` on the `prefer_column_name_to_alias`
-- path. Sanitizer builds (MSan/ASan) catch the use-after-destroy of the empty set.

DROP FUNCTION IF EXISTS udf_normalizer_04280;

CREATE FUNCTION udf_normalizer_04280 AS (x) -> ((x + 1 AS y, y + 2));

SET prefer_column_name_to_alias = 1;
SET skip_redundant_aliases_in_udf = 1;

-- Old (non-analyzer) path: TreeRewriter::normalize -> UserDefinedSQLFunctionVisitor.
SELECT udf_normalizer_04280(4 + 2) SETTINGS enable_analyzer = 0;

-- New-analyzer path still calls the visitor from InterpreterCreateQuery::createTable.
DROP TABLE IF EXISTS udf_normalizer_04280_table;
CREATE TABLE udf_normalizer_04280_table
(
    a Int32,
    b Int32 MATERIALIZED udf_normalizer_04280(a)
) ENGINE = MergeTree ORDER BY a;

DROP TABLE udf_normalizer_04280_table;
DROP FUNCTION udf_normalizer_04280;
