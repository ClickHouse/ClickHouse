-- Arguments of the `vector_search_index_analysis` optimization are evaluated as expressions, so a
-- malformed argument is rejected with `BAD_ARGUMENTS` instead of reaching `IAST::getColumnName`.
DROP TABLE IF EXISTS data;
CREATE TABLE data (key Int, value Int) ENGINE = MergeTree() ORDER BY key;

SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', tuple(materialize(1))); -- { serverError BAD_ARGUMENTS }

-- A subquery argument, which resolves to an expression list rather than a column.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- An argument node with no children.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', 1); -- { serverError BAD_ARGUMENTS }

-- `mergeTreeAnalyzeIndexesUUID` reaches the same code at a different argument index. Arguments are
-- parsed before the table is resolved, so the UUID does not have to exist.
SELECT * FROM mergeTreeAnalyzeIndexesUUID('00000000-0000-0000-0000-000000000001', 1, [], 'vector_search_index_analysis', (SELECT tuple(1))); -- { serverError BAD_ARGUMENTS }

-- A well-formed constant array is still accepted and reaches the arity check.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', CAST([1, 2, 3] AS Array(UInt8))); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A six-element array is decoded element by element, each with its own expected type. `data` has no
-- parts, so the analysis itself returns nothing.
SELECT * FROM mergeTreeAnalyzeIndexes(currentDatabase(), data, 1, [], 'vector_search_index_analysis', array('value', 'L2Distance', 1, [1.0], false, false));

DROP TABLE data;
