-- RENAME is represented by explicit source/target child nodes in AST JSON.
SELECT
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'type'),
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'children', 1, 'source_name'),
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'children', 1, 'target_name'),
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'children', 2, 'source_name'),
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'transformers', 'children', 1, 'children', 2, 'target_name')
FROM (SELECT parseQueryToJSON('SELECT * RENAME (`a b` AS `x y`, a AS b) FROM t') AS j);

SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * RENAME (`a b` AS `x y`, a AS b) FROM t'));

SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","children":[{"type":"ColumnsRenameTransformerRename","source_name":"a","target_name":"x"},{"type":"ColumnsRenameTransformerRename","source_name":"a","target_name":"y"}]}'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","children":[{"type":"ColumnsRenameTransformerRename","source_name":"a","target_name":"x"},{"type":"ColumnsRenameTransformerRename","source_name":"b","target_name":"x"}]}'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatQueryFromJSON('{"type":"ColumnsTransformerList","children":[{"type":"ColumnsRenameTransformer","children":[{"type":"ColumnsRenameTransformerRename","source_name":"a","target_name":"x"}]},{"type":"ColumnsApplyTransformer","func_name":"toString"}]}'); -- { serverError BAD_ARGUMENTS }
