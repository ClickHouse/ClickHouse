-- The parser accepts a tuple type with only some elements named (the data type factory rejects it later),
-- parseQueryToJSON wrote the unnamed elements with empty names and formatQueryFromJSON refused its own output.
-- Found by the JSON round-trip stage of json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (col Tuple(CPU, double, Memory double)) ENGINE = Memory'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT CAST((1, 2) AS Tuple(a UInt8, UInt8))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT CAST((1, 2) AS Tuple(a UInt8, b UInt8))'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT CAST((1, 2) AS Tuple(UInt8, UInt8))'));
-- The count of names must still match the count of elements.
SELECT formatQueryFromJSON('{"type":"TupleDataType","name":"Tuple","arguments":{"type":"ExpressionList","children":[{"type":"DataType","name":"UInt8"}]},"element_names":["a","b"]}'); -- { serverError BAD_ARGUMENTS }
