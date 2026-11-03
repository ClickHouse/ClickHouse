-- Fixed only for analyzer
SET enable_analyzer=1;

INSERT INTO TABLE FUNCTION file(database() || '.test-data.json', JSON)
    SELECT number numeric FROM numbers(10);

CREATE VIEW test_view AS SELECT * FROM file(database() || '.test-data.json', JSON);

CREATE TABLE test_table_view (a String) Engine=Memory AS
    SELECT toString(numeric) FROM test_view;
SELECT COUNT(*) FROM test_table_view;

INSERT INTO test_table_view
    SELECT toString(numeric) FROM test_view;
SELECT COUNT(*) FROM test_table_view;

-- Same without View

CREATE TABLE test_table (a String) Engine=Memory AS
    SELECT toString(numeric) FROM (SELECT * FROM file(database() || '.test-data.json', JSON));
SELECT COUNT(*) FROM test_table;

INSERT INTO test_table
    SELECT toString(numeric) FROM (SELECT * FROM file(database() || '.test-data.json', JSON));
SELECT COUNT(*) FROM test_table;
