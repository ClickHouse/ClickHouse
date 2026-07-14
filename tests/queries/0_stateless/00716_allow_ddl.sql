SET send_logs_level = 'fatal';
SET allow_ddl = 0;

CREATE DATABASE some_db; -- { serverError QUERY_IS_PROHIBITED } 
CREATE TABLE some_table(a Int32) ENGINE = Memory; -- { serverError QUERY_IS_PROHIBITED}
ALTER TABLE some_table DELETE WHERE 1; -- { serverError QUERY_IS_PROHIBITED}
RENAME TABLE some_table TO some_table1; -- { serverError QUERY_IS_PROHIBITED}
SET allow_ddl = 1; -- { serverError QUERY_IS_PROHIBITED}
