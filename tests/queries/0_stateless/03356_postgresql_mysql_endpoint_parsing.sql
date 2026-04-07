-- Tags: no-fasttest
-- We test only that parsing of the endpoint works in this test.
DROP TABLE IF EXISTS tablefunc01;
DROP TABLE IF EXISTS tablefunc02;
CREATE TABLE tablefunc01 (x int) AS postgresql('localhost:9005/postgresql', 'postgres_db', 'postgres_table', 'postgres_user', '124444');
CREATE TABLE tablefunc02 (x int) AS mysql('127.0.0.1:9004/mysql', 'mysql_db', 'mysql_table', 'mysql_user','123123');
