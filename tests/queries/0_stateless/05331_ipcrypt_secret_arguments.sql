-- Tags: no-fasttest
-- Tag no-fasttest: `ipcrypt2` is not built in the fast test

-- The key argument of the `ipcrypt*` functions must be hidden in `system.query_log`.

SET enable_ipcrypt = 1;

SELECT ipcryptEncrypt(toIPv4('192.168.1.1'), '0123456789abcdef0123456789abcdef') FORMAT Null;
SELECT ipcryptDecrypt('::1', '0123456789abcdef0123456789abcdef') FORMAT Null;
SELECT ipcryptPrefixEncrypt(toIPv4('192.168.1.1'), '00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100') FORMAT Null;
SELECT ipcryptPrefixDecrypt('10.0.0.1', '00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100') FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT query FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT ipcrypt%'
ORDER BY event_time_microseconds;

-- A malformed named `key` argument written before the positional span must be hidden too.
SELECT ipcryptEncrypt(key = '0123456789abcdef0123456789abcdef', '1.2.3.4') FORMAT Null; -- { serverError UNKNOWN_IDENTIFIER }
SELECT ipcryptDecrypt(key = '0123456789abcdef0123456789abcdef') FORMAT Null; -- { serverError UNKNOWN_IDENTIFIER }

SYSTEM FLUSH LOGS query_log;

SELECT query FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionBeforeStart' AND query LIKE 'SELECT ipcrypt%'
ORDER BY event_time_microseconds;
