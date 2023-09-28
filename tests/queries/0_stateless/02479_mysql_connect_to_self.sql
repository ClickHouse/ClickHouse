-- Tags: no-fasttest

SET send_logs_level = 'fatal'; -- failed connection tries are ok, if it succeeded after retry.

CREATE TABLE foo (key UInt32, a String, b Int64, c String) ENGINE = TinyLog;
INSERT INTO foo VALUES (1, 'one', -1, 'een'), (2, 'two', -2, 'twee'), (3, 'three', -3, 'drie'), (4, 'four', -4, 'vier'), (5, 'five', -5, 'vijf');

SET allow_experimental_analyzer = 1;

SELECT '---';
SELECT * FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '') ORDER BY key;

SELECT '---';
SELECT count() FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '');

SELECT '---';
SELECT 1 FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '');

SELECT '---';
SELECT key FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '') ORDER BY key;

SELECT '---';
SELECT b, a FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '') ORDER BY a;

SELECT '---';
SELECT b, a FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '') ORDER BY c;

SELECT '---';
SELECT b FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '') WHERE c != 'twee' ORDER BY b;

SELECT '---';
SELECT count() FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '') WHERE c != 'twee';

EXPLAIN QUERY TREE dump_ast = 1
SELECT * FROM mysql(
    '127.0.0.1:9004', currentDatabase(), foo, 'default', '',
    SETTINGS connection_wait_timeout = 123, connect_timeout = 40123002, read_write_timeout = 40123001, connection_pool_size = 3
);

SELECT '---';
SELECT count() FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '', SETTINGS connection_pool_size = 1);
SELECT count() FROM mysql('127.0.0.1:9004', currentDatabase(), foo, 'default', '', SETTINGS connection_pool_size = 0); -- { serverError BAD_ARGUMENTS }
