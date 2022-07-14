-- Tags: no-parallel

CREATE OR REPLACE FUNCTION _02101_test_function AS x -> x + 1;

SELECT create_query FROM system.functions WHERE name = '_02101_test_function';
SELECT _02101_test_function(1);

CREATE OR REPLACE FUNCTION _02101_test_function AS x -> x + 2;

SELECT create_query FROM system.functions WHERE name = '_02101_test_function';
SELECT _02101_test_function(1);

DROP FUNCTION _02101_test_function;
