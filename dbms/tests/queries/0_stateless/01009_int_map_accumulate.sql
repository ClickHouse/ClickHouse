SET send_logs_level = 'none';

DROP TABLE IF EXISTS accum;
CREATE TABLE accum(key Array(UInt32), value Array(UInt32)) ENGINE = Log;
INSERT INTO accum SELECT [1,2,number], [1,2,3] FROM numbers(3,5);
SELECT mapAccumulate(key, value) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT mapAccumulate(key, value, 6) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT mapAccumulate(key, value, key[3] - 1) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT mapAccumulate(key, [1,1,1]) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT mapAccumulate([2,3,key[3] +1], value) FROM (SELECT key, value FROM accum ORDER BY key);

DROP TABLE accum;

SELECT mapAccumulate([toInt8(1), 2, toInt8(number)], [toInt8(1),1,1]) FROM numbers(3,2);
SELECT mapAccumulate([toInt8(1), 2, toInt8(number)], [toUInt8(1),1,1]) FROM numbers(3,2);
SELECT mapAccumulate([toUInt32(1), 2, toUInt32(number)], [toInt8(1),1,1]) FROM numbers(3,2);
SELECT mapAccumulate([toUInt32(1), 2, toUInt32(number)], [toUInt16(1),1,1]) FROM numbers(3,2);
SELECT mapAccumulate([toInt32(1), 2, toInt32(number)], [1.1,1,1]) FROM numbers(3,2);
SELECT mapAccumulate([toUInt64(1), 2, toUInt64(number)], [1.1,1,1]) FROM numbers(3,2);

SELECT mapAccumulate([1.1, 2, number + 1.0], [1.1,1,1]) FROM numbers(3,2); -- { serverError 43 }
SELECT mapAccumulate([1, 2, number], ['1','2','3']) FROM numbers(3,2); -- { serverError 43 }
