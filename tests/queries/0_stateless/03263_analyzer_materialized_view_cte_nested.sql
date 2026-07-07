SET enable_analyzer = 1;

DROP TABLE IF EXISTS test_table;
DROP VIEW IF EXISTS test_mv;

CREATE TABLE test_table ENGINE = MergeTree ORDER BY tuple() AS SELECT 1 as col1;

CREATE MATERIALIZED VIEW test_mv ENGINE = MergeTree ORDER BY tuple() AS
WITH
    subquery_on_source AS (SELECT col1 AS aliased FROM test_table),
    output AS (SELECT * FROM test_table WHERE col1 IN (SELECT aliased FROM subquery_on_source))
SELECT * FROM output;

INSERT INTO test_table VALUES (2);

SELECT * FROM test_mv;

DROP VIEW test_mv;
DROP TABLE test_table;
