-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

-- A scalar subquery used as the URL argument of a table function must be evaluated
-- in CREATE TABLE ... AS SELECT. The structure of the SELECT is inferred in
-- only-analyze mode, but the table function is still resolved into a storage there,
-- so it needs the real URL. Used to fail with "Host is empty in S3 URI".

SET enable_analyzer = 1;

DROP TABLE IF EXISTS path_table;
DROP TABLE IF EXISTS s3_table_cte;
DROP TABLE IF EXISTS s3_table_inline;

CREATE TABLE path_table ENGINE = MergeTree ORDER BY () AS
SELECT 'http://localhost:11111/test/a.tsv' AS s3path;

SELECT '-- SELECT with a CTE scalar as the URL';
WITH (SELECT s3path FROM path_table) AS s3_url
SELECT * FROM s3(s3_url, NOSIGN, 'TSV') ORDER BY c1;

SELECT '-- CREATE TABLE ... AS SELECT with a CTE scalar as the URL';
CREATE TABLE s3_table_cte ENGINE = MergeTree ORDER BY () AS
WITH (SELECT s3path FROM path_table) AS s3_url
SELECT * FROM s3(s3_url, NOSIGN, 'TSV');
SELECT * FROM s3_table_cte ORDER BY c1;

SELECT '-- CREATE TABLE ... AS SELECT with an inline scalar as the URL';
CREATE TABLE s3_table_inline ENGINE = MergeTree ORDER BY () AS
SELECT * FROM s3((SELECT s3path FROM path_table), NOSIGN, 'TSV');
SELECT * FROM s3_table_inline ORDER BY c1;

DROP TABLE path_table;
DROP TABLE s3_table_cte;
DROP TABLE s3_table_inline;
