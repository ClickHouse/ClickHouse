-- Arguments of the `vector_search_index_analysis` optimization are evaluated as expressions, so a
-- malformed argument is rejected with `BAD_ARGUMENTS` instead of reaching `IAST::getColumnName`.
DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int, value Int) ENGINE = MergeTree() ORDER BY key;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', tuple(materialize(1))); -- { serverError BAD_ARGUMENTS }

-- A subquery argument, which resolves to an expression list rather than a column.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- An argument node with no children.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', 1); -- { serverError BAD_ARGUMENTS }

-- `mergeTreeAnalyzeIndexesUUID` reaches the same code at a different argument index.
SELECT * FROM mergeTreeAnalyzeIndexesUUID((SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'data'), 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- A well-formed constant array is still accepted and reaches the arity check.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', CAST([1, 2, 3] AS Array(UInt8))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE data;
