DROP VIEW IF EXISTS test_05325_active_employees;
DROP TABLE IF EXISTS test_05325_employees;

CREATE TABLE test_05325_employees
(
    id UInt64,
    name String,
    surname String,
    disabled Bool
)
ENGINE = Memory;

INSERT INTO test_05325_employees VALUES
    (1, 'Alice', 'Smith', false),
    (2, 'Bob', 'Adams', false),
    (3, 'Ann', 'Baker', true),
    (4, 'Carol', 'Able', false);

CREATE VIEW test_05325_active_employees AS
SELECT id, name, surname
FROM test_05325_employees
WHERE NOT disabled AND (name LIKE {name_pattern:String} OR surname LIKE {name_pattern:String});

SET param_table = 'test_05325_active_employees';
SET param_name_pattern = 'A%';

SELECT id, name
FROM {table:Identifier}(name_pattern = {name_pattern:String})
ORDER BY id;

SELECT id, name
FROM {table:Identifier}(name_pattern = 'B%') AS active
ORDER BY id;

SELECT formatQueryFromJSON(parseQueryToJSON(
    'SELECT * FROM {table:Identifier}(name_pattern = {name_pattern:String})'));

SELECT * FROM {missing_table:Identifier}(name_pattern = 'A%'); -- { serverError UNKNOWN_QUERY_PARAMETER }

DESCRIBE TABLE {table:Identifier}(name_pattern = 'A%') SETTINGS describe_compact_output = 1;

-- The bare `SELECT` argument grammar of `view`, `viewIfPermitted` and `eval` is not available through a placeholder.
SELECT * FROM {table:Identifier}(SELECT 1); -- { clientError SYNTAX_ERROR }
SET param_table = 'view';
SELECT * FROM {table:Identifier}((SELECT 1)); -- { serverError BAD_ARGUMENTS }
SET param_table = 'eval';
SELECT * FROM {table:Identifier}('SELECT 42') SETTINGS allow_experimental_eval_table_function = 1;

-- A stored parameterized view keeps the placeholder, so the function is unknown and every argument may be a secret.
SET format_display_secrets_in_show_and_select = 0;
CREATE VIEW test_05325_secret AS SELECT * FROM {f:Identifier}('localhost:9000', 'db', 't', 'u', 'secret');
SHOW CREATE TABLE test_05325_secret;
SELECT create_table_query FROM system.tables WHERE database = currentDatabase() AND name = 'test_05325_secret';
DROP VIEW test_05325_secret;

SELECT count() FROM numbers(1);

DROP VIEW test_05325_active_employees;
DROP TABLE test_05325_employees;
