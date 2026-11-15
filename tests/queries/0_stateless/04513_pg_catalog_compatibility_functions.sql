-- Compatibility functions for PostgreSQL clients (used by psql for the \d command).

SELECT pg_table_is_visible(0), pg_table_is_visible(123456);
SELECT pgTableIsVisible(1);
SELECT PG_TABLE_IS_VISIBLE(1);

SELECT pg_get_userbyid(10);
SELECT pgGetUserById(10);
SELECT pg_get_userbyid(10) = currentUser();

SELECT pg_table_is_visible(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT pg_get_userbyid(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
