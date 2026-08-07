-- Tags: no-fasttest
-- Regression test: remote() with input() table function as argument
-- used to throw LOGICAL_ERROR "Bad cast from type DB::TableFunctionNode to DB::QueryNode"
-- prefer_localhost_replica = 1 is needed because input() cannot forward INSERT data over the network protocol
SET enable_analyzer = 1;
INSERT INTO FUNCTION null() SELECT * FROM remote('127.0.0.1', input('x String')) SETTINGS prefer_localhost_replica = 1 FORMAT LineAsString
test
