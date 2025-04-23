CREATE DATABASE IF NOT EXISTS test;

 CREATE TABLE IF NOT EXISTS test.simple_table
(
    id   UInt64,
    name String
)
ENGINE = Memory;


INSERT INTO test.simple_table
SELECT
    1 AS id,
    'Alice' AS name;

INSERT INTO test.simple_table
SELECT
    number + 1 AS id,
    ['Alice','Bob','Charlie'][number + 1] AS name
FROM numbers(3);