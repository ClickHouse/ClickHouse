CREATE DATABASE IF NOT EXISTS test;
DROP TABLE IF EXISTS test.runs;
DROP TABLE IF EXISTS test.tests;

CREATE TABLE test.runs
(
    date Date, 
    id UInt64, 
    t_id UInt64, 
    status Enum8('OK' = 1, 'FAILED' = 2, 'SKIPPED' = 3, 'DISCOVERED' = 4), 
    run_id UInt64 DEFAULT id
) ENGINE = MergeTree(date, (t_id, id), 8192);

CREATE TABLE test.tests
(
    date Date, 
    id UInt64, 
    path Nullable(String), 
    suite_id Nullable(String)
) ENGINE = MergeTree(date, id, 8192);

INSERT INTO test.tests (date, id) VALUES (1,1);
INSERT INTO test.runs (date, id) VALUES (1,1);
INSERT INTO test.runs (date, id, status) VALUES (1,2, 'FAILED');
INSERT INTO test.tests (date, id, path) VALUES (1,2 ,'rtline1');

SELECT *
FROM test.runs AS r
WHERE (r.status = 'FAILED') AND (
(
    SELECT path
    FROM test.tests AS t
    WHERE t.id = r.id
    LIMIT 1
) LIKE 'rtline%')
LIMIT 1;

SELECT 'still alive';

DROP TABLE test.runs;
DROP TABLE test.tests;
