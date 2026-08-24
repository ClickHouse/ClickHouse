CREATE TABLE foo (key UInt32, a String, b Int64, c String) ENGINE = TinyLog;

SELECT count() FROM mysql(
    mysql('127.0.0.1:9004', currentDatabase(), 'foo', 'default', ''),
    '127.0.0.1:9004', currentDatabase(), 'foo', '', '',
    SETTINGS connect_timeout = 100, connection_wait_timeout = 100, read_write_timeout = 300); -- { serverError UNKNOWN_FUNCTION }
SELECT count() FROM mysql(
    mysql('127.0.0.1:9004', currentDatabase(), 'foo', 'default', '', SETTINGS connection_pool_size = 1),
    '127.0.0.1:9004', currentDatabase(), 'foo', '', '',
    SETTINGS connect_timeout = 100, connection_wait_timeout = 100, read_write_timeout = 300); -- { serverError UNKNOWN_FUNCTION, UNSUPPORTED_METHOD }
