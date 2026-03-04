-- Tags: no-fasttest
-- Regression test: remote() with input() table function as argument
-- used to throw LOGICAL_ERROR "Bad cast from type DB::TableFunctionNode to DB::QueryNode"
INSERT INTO FUNCTION null() SELECT * FROM remote('127.0.0.1', input('x String')) FORMAT LineAsString
test
