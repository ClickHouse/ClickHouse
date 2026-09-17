
CREATE OR REPLACE FUNCTION test_02101_create_or_replace_function AS x -> x + 1;

SELECT create_query FROM system.functions WHERE name = 'test_02101_create_or_replace_function';
SELECT test_02101_create_or_replace_function(1);

CREATE OR REPLACE FUNCTION test_02101_create_or_replace_function AS x -> x + 2;

SELECT create_query FROM system.functions WHERE name = 'test_02101_create_or_replace_function';
SELECT test_02101_create_or_replace_function(1);

DROP FUNCTION test_02101_create_or_replace_function;
