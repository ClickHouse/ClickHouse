SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * RENAME a AS x FROM t')) FORMAT TSVRaw;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * RENAME (a AS x, b AS y) FROM t')) FORMAT TSVRaw;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * RENAME `a b` AS `x.y` FROM t')) FORMAT TSVRaw;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * APPLY(sum) RENAME (col -> concat(col, \'_sum\')) FROM t')) FORMAT TSVRaw;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT t.COLUMNS(\'a\') RENAME a AS x FROM t')) FORMAT TSVRaw;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT COLUMNS(a, b) RENAME (a AS b, b AS a) FROM t')) FORMAT TSVRaw;

SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","source_names":["a"],"target_names":[]}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","source_names":["a"],"target_names":["x","y"]}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","source_names":[1],"target_names":["x"]}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","source_names":["a"],"target_names":[""]}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","source_names":["a"],"target_names":["x"],"lambda_arg":"col"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","lambda":{"type":"Identifier","name":"col"},"lambda_arg":"col"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * RENAME (col -> col) FROM t'), '"lambda_arg":"col"', '"lambda_arg":"other"')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * RENAME (col -> col) FROM t'), '"lambda_arg":"col"', '"lambda_arg":""')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * RENAME (col -> col) FROM t'), '"lambda_arg":"col"', '"lambda_arg":"col","source_names":["a"],"target_names":["x"]')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsRenameTransformer","lambda":{"type":"Function","name":"lambda","is_lambda_function":true,"arguments":{"type":"ExpressionList","children":[{"type":"Function","name":"tuple","arguments":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"Identifier","name":"b"}]}},{"type":"Identifier","name":"a"}]}},"lambda_arg":"a"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsTransformerList","children":[{"type":"ColumnsRenameTransformer","source_names":["a"],"target_names":["x"]},{"type":"ColumnsApplyTransformer","func_name":"sum"}]}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ColumnsTransformerList","children":[{"type":"ColumnsRenameTransformer","source_names":["a"],"target_names":["x"]},{"type":"ColumnsRenameTransformer","source_names":["x"],"target_names":["y"]}]}'); -- { serverError BAD_ARGUMENTS }
