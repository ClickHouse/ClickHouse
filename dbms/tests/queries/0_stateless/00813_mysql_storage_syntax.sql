SET send_logs_level = 'none';

CREATE TABLE test.test_bad_mysql_table(day Date, xxx String) ENGINE = MySQL(); -- { serverError 42 }
CREATE TABLE test.test_bad_mysql_table(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database_name', 'table_name', 'user_name', 'password', 1, 'xxx = 5'); -- { serverError 36 }
CREATE TABLE test.test_bad_mysql_table(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database_name', 'table_name', 'user_name', 'password') SETTINGS bad_settings = '111'; -- { serverError 36 }
CREATE TABLE test.test_bad_mysql_table(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database_name', 'table_name', 'user_name', 'password') SETTINGS remote_database = 'database_name'; -- { serverError 36 }

DROP TABLE IF EXISTS test.test_mysql_session_variables;
DROP TABLE IF EXISTS test.test_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_no_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_1;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_2;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_3;

CREATE TABLE test.test_mysql_session_variables(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password') SETTINGS mysql_variable_aaa = 'AAA', mysql_variable_bbb = 1;
CREATE TABLE test.test_mapping_database_and_table(day Date, xxx String) ENGINE = MySQL() SETTINGS remote_address = '127.0.0.1', user = 'user_name', password = 'password';
CREATE TABLE test.test_no_mapping_database_and_table(day Date, xxx String) ENGINE = MySQL() SETTINGS remote_address = '127.0.0.1', remote_database = 'database', remote_table_name = 'table', user = 'user_name', password = 'password';
CREATE TABLE test.test_mysql_table_compatibility_1(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password');
CREATE TABLE test.test_mysql_table_compatibility_2(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password', 1);
CREATE TABLE test.test_mysql_table_compatibility_3(day Date, xxx String) ENGINE = MySQL('127.0.0.1', 'database', 'table', 'user_name', 'password', 0, 'xxx = 5');

DROP TABLE IF EXISTS test.test_mysql_session_variables;
DROP TABLE IF EXISTS test.test_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_no_mapping_database_and_table;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_1;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_2;
DROP TABLE IF EXISTS test.test_mysql_table_compatibility_3;