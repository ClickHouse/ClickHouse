SET max_rows_to_read = 50000;

SELECT count() FROM test.hits WHERE -CounterID = -34;
SELECT count() FROM test.hits WHERE abs(-CounterID) = 34;
SELECT count() FROM test.hits WHERE -abs(CounterID) = -34;
SELECT count() FROM test.hits WHERE toUInt32(CounterID) = 34;
SELECT count() FROM test.hits WHERE toInt32(CounterID) = 34;
SELECT count() FROM test.hits WHERE toFloat32(CounterID) = 34;

SET max_rows_to_read = 0;

SELECT count() FROM test.hits WHERE toInt16(CounterID) = 34;
SELECT count() FROM test.hits WHERE toInt8(CounterID) = 34;

SELECT count() FROM test.hits WHERE toDate(toUInt16(CounterID)) = toDate(34);

SELECT uniq(CounterID), uniqUpTo(5)(toInt8(CounterID)), count() FROM test.hits WHERE toInt8(CounterID + 1 - 1) = 34;
SELECT uniq(CounterID), uniqUpTo(5)(toInt8(CounterID)), count() FROM test.hits WHERE toInt8(CounterID) = 34;

SELECT uniq(CounterID), uniqUpTo(5)(toInt16(CounterID)), count() FROM test.hits WHERE toInt16(CounterID + 1 - 1) = 34;
SELECT uniq(CounterID), uniqUpTo(5)(toInt16(CounterID)), count() FROM test.hits WHERE toInt16(CounterID) = 34;

SET max_rows_to_read = 500000;

SELECT uniq(CounterID), count() FROM test.hits WHERE toString(CounterID) = '34';

SET max_rows_to_read = 2000000;

SELECT count() FROM test.hits WHERE CounterID < 101500;
SELECT count() FROM test.hits WHERE CounterID <= 101500;
SELECT count() FROM test.hits WHERE CounterID < 101500 AND CounterID > 42;
SELECT count() FROM test.hits WHERE CounterID < 101500 AND CounterID >= 42;
SELECT count() FROM test.hits WHERE CounterID <= 101500 AND CounterID > 42;
SELECT count() FROM test.hits WHERE CounterID <= 101500 AND CounterID >= 42;
SELECT count() FROM test.hits WHERE -CounterID > -101500;
SELECT count() FROM test.hits WHERE -CounterID >= -101500;
SELECT count() FROM test.hits WHERE -CounterID > -101500 AND CounterID > 42;
SELECT count() FROM test.hits WHERE -CounterID > -101500 AND CounterID >= 42;
SELECT count() FROM test.hits WHERE -CounterID >= -101500 AND CounterID > 42;
SELECT count() FROM test.hits WHERE -CounterID >= -101500 AND CounterID >= 42;
SELECT count() FROM test.hits WHERE CounterID < 101500 AND -CounterID < -42;
SELECT count() FROM test.hits WHERE CounterID < 101500 AND -CounterID <= -42;
SELECT count() FROM test.hits WHERE CounterID <= 101500 AND -CounterID < -42;
SELECT count() FROM test.hits WHERE CounterID <= 101500 AND -CounterID <= -42;

SET max_rows_to_read = 0;

SELECT count() FROM test.hits WHERE EventDate = '2014-03-20';
SELECT count() FROM test.hits WHERE toDayOfMonth(EventDate) = 20;
SELECT count() FROM test.hits WHERE toDayOfWeek(EventDate) = 4;
SELECT count() FROM test.hits WHERE toUInt16(EventDate) = toUInt16(toDate('2014-03-20'));
SELECT count() FROM test.hits WHERE toInt64(EventDate) = toInt64(toDate('2014-03-20'));
SELECT count() FROM test.hits WHERE toDateTime(EventDate) = '2014-03-20 00:00:00';

SET max_rows_to_read = 50000;

SELECT count() FROM test.hits WHERE toMonth(EventDate) != 3;
SELECT count() FROM test.hits WHERE toYear(EventDate) != 2014;
SELECT count() FROM test.hits WHERE toDayOfMonth(EventDate) > 23 OR toDayOfMonth(EventDate) < 17;
