DROP TEMPORARY TABLE IF EXISTS table_to_drop;
DROP TABLE IF EXISTS table_to_drop;

CREATE TABLE table_to_drop(x Int8) ENGINE=Log;
CREATE TEMPORARY TABLE table_to_drop(x Int8);
DROP TEMPORARY TABLE table_to_drop;
DROP TEMPORARY TABLE table_to_drop; -- { serverError UNKNOWN_TABLE }
DROP TABLE table_to_drop;
DROP TABLE table_to_drop; -- { serverError UNKNOWN_TABLE }

CREATE TABLE table_to_drop(x Int8) ENGINE=Log;
CREATE TEMPORARY TABLE table_to_drop(x Int8);
DROP TABLE table_to_drop;
DROP TABLE table_to_drop;
DROP TABLE table_to_drop; -- { serverError UNKNOWN_TABLE }
