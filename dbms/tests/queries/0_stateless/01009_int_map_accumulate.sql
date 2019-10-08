SET send_logs_level = 'none';

DROP TABLE IF EXISTS accum;
CREATE TABLE accum(key Array(UInt32), value Array(UInt32)) ENGINE = Log;
INSERT INTO accum SELECT [1,2,number], [1,2,3] FROM numbers(3,5);
SELECT fillSortedArraysCumulative(key, value) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT fillSortedArraysCumulative(key, value, 6) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT fillSortedArraysCumulative(key, value, key[3] - 1) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT fillSortedArraysCumulative(key, [1,1,1]) FROM (SELECT key, value FROM accum ORDER BY key);
SELECT fillSortedArraysCumulative([2,3,key[3] +1], value) FROM (SELECT key, value FROM accum ORDER BY key);

DROP TABLE accum;

SELECT fillSortedArraysCumulative([toInt8(1), 2, toInt8(number)], [toInt8(1),1,1]) FROM numbers(3,2);
SELECT fillSortedArraysCumulative([toInt8(1), 2, toInt8(number)], [toUInt8(1),1,1]) FROM numbers(3,2);
SELECT fillSortedArraysCumulative([toUInt32(1), 2, toUInt32(number)], [toInt8(1),1,1]) FROM numbers(3,2);
SELECT fillSortedArraysCumulative([toUInt32(1), 2, toUInt32(number)], [toUInt16(1),1,1]) FROM numbers(3,2);
SELECT fillSortedArraysCumulative([toInt32(1), 2, toInt32(number)], [1.1,1,1]) FROM numbers(3,2);
SELECT fillSortedArraysCumulative([toUInt64(1), 2, toUInt64(number)], [1.1,1,1]) FROM numbers(3,2);

SELECT fillSortedArraysCumulative([1.1, 2, number + 1.0], [1.1,1,1]) FROM numbers(3,2); -- { serverError 43 }
SELECT fillSortedArraysCumulative([1, 2, number], ['1','2','3']) FROM numbers(3,2); -- { serverError 43 }
