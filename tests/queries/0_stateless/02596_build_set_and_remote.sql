-- {echoOn}
SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM system.one;

SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}', system.one);
SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}');

SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}', system.one) GROUP BY NULL;
SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}') GROUP BY NULL;

SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}', system.one) GROUP BY 1;
SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}') GROUP BY 1;

SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}', system.one) GROUP BY 'A';
SELECT arrayExists(x -> (x IN (SELECT '2')), [2]) FROM remote('127.0.0.{2,3}') GROUP BY 'A';

SELECT 1 IN ( SELECT 1 ) FROM remote('127.0.0.{1,2}', system.one) GROUP BY dummy;
SELECT 1 IN ( SELECT 1 ) FROM remote('127.0.0.{1,2}') GROUP BY dummy;

SELECT 1000.0001, toUInt64(arrayJoin([NULL, 257, 65536, NULL])), arrayExists(x -> (x IN (SELECT '2.55')), [-9223372036854775808]) FROM remote('127.0.0.{1,2}', system.one) GROUP BY NULL, NULL, NULL, NULL;
SELECT 1000.0001, toUInt64(arrayJoin([NULL, 257, 65536, NULL])), arrayExists(x -> (x IN (SELECT '2.55')), [-9223372036854775808]) FROM remote('127.0.0.{1,2}') GROUP BY NULL, NULL, NULL, NULL;
