-- Tags: no-fasttest
-- no-fasttest because support for PostgreSQL is not included for fast tests

SELECT '--Some statements that should throw argument exception because they do not have valid parameters';

SELECT '--Database name should be valid';
SELECT * FROM postgresql('127.0.0.1:5432', '1postgres_db', 'postgres_table', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db ', 'postgres_table', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db)', 'postgres_table', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db\'', 'postgres_table', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', "postgres_db\'", 'postgres_table', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }

SELECT '--Table name should be valid';
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db', '1postgres_table', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db', 'postgres_table ', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db', 'postgres_table)', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db', 'postgres_table\'', 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM postgresql('127.0.0.1:5432', 'postgres_db', "postgres_table\'", 'postgres_user', '124444'); -- { serverError BAD_ARGUMENTS }

SELECT '--Named collections are also checked';
DROP NAMED COLLECTION IF EXISTS psql;
CREATE NAMED COLLECTION psql AS host = '127.0.0.1', port = 5432, database = 'postgres_db\'', table = 'postgres_table', user = 'postgres_user', password = '124444';
SELECT * FROM postgresql(psql); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION psql;

CREATE NAMED COLLECTION psql AS host = '127.0.0.1', port = 5432, database = 'postgres_db', table = 'postgres_table\'', user = 'postgres_user', password = '124444';
SELECT * FROM postgresql(psql); -- { serverError BAD_ARGUMENTS }
DROP NAMED COLLECTION psql;
