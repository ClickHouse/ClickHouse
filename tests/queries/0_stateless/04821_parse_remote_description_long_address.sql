-- Tags: no-fasttest

-- Parsing the address of an external-database table function used to be quadratic
-- in the length of the description, so a fuzzed 1 MiB zero-padded `FixedString`
-- address made the query hang during analysis, before it could be cancelled.
-- The trailing zero bytes end up in the port part, which ignores trailing garbage,
-- so the query below connects to the server itself and must complete quickly.

SET send_logs_level = 'fatal'; -- failed connection tries are ok, if it succeeded after retry.

DROP TABLE IF EXISTS foo;
CREATE TABLE foo (key UInt32) ENGINE = TinyLog;
INSERT INTO foo VALUES (1), (2), (3);

SELECT count() FROM mysql(toFixedString('127.0.0.1:9004', 1048575), currentDatabase(), foo, 'default', '', SETTINGS connect_timeout = 100, connection_wait_timeout = 100);

DROP TABLE foo;
