DROP VIEW IF EXISTS test_04658_active_employees;
DROP TABLE IF EXISTS test_04658_employees;

CREATE TABLE test_04658_employees
(
    id UInt64,
    name String,
    surname String,
    disabled Bool
)
ENGINE = Memory;

INSERT INTO test_04658_employees VALUES
    (1, 'Alice', 'Smith', false),
    (2, 'Bob', 'Adams', false),
    (3, 'Ann', 'Baker', true),
    (4, 'Carol', 'Able', false);

CREATE VIEW test_04658_active_employees AS
SELECT id, name, surname
FROM test_04658_employees
WHERE NOT disabled AND (name LIKE {name_pattern:String} OR surname LIKE {name_pattern:String});

SET param_table = 'test_04658_active_employees';
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

SELECT count() FROM numbers(1);

DROP VIEW test_04658_active_employees;
DROP TABLE test_04658_employees;
