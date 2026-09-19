-- JSONObjectEachRow accepted bytes after the closing `}` when the object end was reached as the first row of a
-- block (max_block_size = 1) and rejected them otherwise. Found by json_ast_sql_execution_fuzzer (differential oracle).
SELECT * FROM format(JSONObjectEachRow, '{"x":{"y":1,"z":2}}, {}') SETTINGS max_block_size = 1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONObjectEachRow, '{"x":{"y":1,"z":2}}, {}') SETTINGS max_block_size = 65536; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT * FROM format(JSONObjectEachRow, '{"x":{"y":1},"w":{"y":2}} garbage') SETTINGS max_block_size = 1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
-- Well-formed input is unaffected by the block size.
SELECT * FROM format(JSONObjectEachRow, '{"x":{"y":1,"z":2},"w":{"y":3,"z":4}}') ORDER BY y SETTINGS max_block_size = 1;
SELECT * FROM format(JSONObjectEachRow, '{"x":{"y":1,"z":2},"w":{"y":3,"z":4}}   ') ORDER BY y SETTINGS max_block_size = 65536;
SELECT * FROM format(JSONObjectEachRow, 'y UInt8', '{}') SETTINGS max_block_size = 1;
