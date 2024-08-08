SELECT('Comparing nanoseconds');
SELECT toIntervalNanosecond(500) > toIntervalNanosecond(300);
SELECT toIntervalNanosecond(1000) < toIntervalNanosecond(1500);
SELECT toIntervalNanosecond(2000) = toIntervalNanosecond(2000);
SELECT toIntervalNanosecond(1000) >= toIntervalMicrosecond(1);
SELECT toIntervalNanosecond(1000001) > toIntervalMillisecond(1);
SELECT toIntervalNanosecond(2000000001) > toIntervalSecond(2);
SELECT toIntervalNanosecond(60000000000) = toIntervalMinute(1);
SELECT toIntervalNanosecond(7199999999999) < toIntervalHour(2);
SELECT toIntervalNanosecond(1) < toIntervalDay(2);
SELECT toIntervalNanosecond(5) < toIntervalWeek(1);

SELECT toIntervalNanosecond(500) < toIntervalNanosecond(300);
SELECT toIntervalNanosecond(1000) > toIntervalNanosecond(1500);
SELECT toIntervalNanosecond(2000) != toIntervalNanosecond(2000);
SELECT toIntervalNanosecond(1000) < toIntervalMicrosecond(1);
SELECT toIntervalNanosecond(1000001) < toIntervalMillisecond(1);
SELECT toIntervalNanosecond(2000000001) < toIntervalSecond(2);
SELECT toIntervalNanosecond(60000000000) != toIntervalMinute(1);
SELECT toIntervalNanosecond(7199999999999) > toIntervalHour(2);
SELECT toIntervalNanosecond(1) > toIntervalDay(2);
SELECT toIntervalNanosecond(5) > toIntervalWeek(1);

SELECT toIntervalNanosecond(1) < toIntervalMonth(2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing microseconds');
SELECT toIntervalMicrosecond(1) < toIntervalMicrosecond(999);
SELECT toIntervalMicrosecond(1001) > toIntervalMillisecond(1);
SELECT toIntervalMicrosecond(2000000) = toIntervalSecond(2);
SELECT toIntervalMicrosecond(179999999) < toIntervalMinute(3);
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
SELECT toIntervalMicrosecond(36000000000000) > toIntervalDay(2);
SELECT toIntervalMicrosecond(1209600000000) = toIntervalWeek(2);

SELECT toIntervalMicrosecond(1) > toIntervalMicrosecond(999);
SELECT toIntervalMicrosecond(1001) < toIntervalMillisecond(1);
SELECT toIntervalMicrosecond(2000000) != toIntervalSecond(2);
SELECT toIntervalMicrosecond(179999999) > toIntervalMinute(3);
SELECT toIntervalMicrosecond(3600000000) != toIntervalHour(1);
SELECT toIntervalMicrosecond(36000000000000) < toIntervalDay(2);
SELECT toIntervalMicrosecond(1209600000000) != toIntervalWeek(2);

SELECT toIntervalMicrosecond(36000000000000) < toIntervalQuarter(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing milliseconds');
SELECT toIntervalMillisecond(2000) > toIntervalMillisecond(2);
SELECT toIntervalMillisecond(2000) = toIntervalSecond(2);
SELECT toIntervalMillisecond(170000) < toIntervalMinute(3);
SELECT toIntervalMillisecond(144000001) > toIntervalHour(40);
SELECT toIntervalMillisecond(1728000000) = toIntervalDay(20);
SELECT toIntervalMillisecond(1198599999) < toIntervalWeek(2);

SELECT toIntervalMillisecond(2000) < toIntervalMillisecond(2);
SELECT toIntervalMillisecond(2000) != toIntervalSecond(2);
SELECT toIntervalMillisecond(170000) > toIntervalMinute(3);
SELECT toIntervalMillisecond(144000001) < toIntervalHour(40);
SELECT toIntervalMillisecond(1728000000) != toIntervalDay(20);
SELECT toIntervalMillisecond(1198599999) > toIntervalWeek(2);

SELECT toIntervalMillisecond(36000000000000) < toIntervalYear(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing seconds');
SELECT toIntervalSecond(120) > toIntervalSecond(2);
SELECT toIntervalSecond(120) = toIntervalMinute(2);
SELECT toIntervalSecond(1) < toIntervalHour(2);
SELECT toIntervalSecond(86401) >= toIntervalDay(1);
SELECT toIntervalSecond(1209600) = toIntervalWeek(2);

SELECT toIntervalSecond(120) < toIntervalSecond(2);
SELECT toIntervalSecond(120) != toIntervalMinute(2);
SELECT toIntervalSecond(1) > toIntervalHour(2);
SELECT toIntervalSecond(86401) < toIntervalDay(1);
SELECT toIntervalSecond(1209600) != toIntervalWeek(2);

SELECT toIntervalSecond(36000000000000) < toIntervalMonth(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing minutes');
SELECT toIntervalMinute(1) < toIntervalMinute(59);
SELECT toIntervalMinute(1) < toIntervalHour(59);
SELECT toIntervalMinute(1440) = toIntervalDay(1);
SELECT toIntervalMinute(30241) > toIntervalWeek(3);

SELECT toIntervalMinute(1) > toIntervalMinute(59);
SELECT toIntervalMinute(1) > toIntervalHour(59);
SELECT toIntervalMinute(1440) != toIntervalDay(1);
SELECT toIntervalMinute(30241) < toIntervalWeek(3);

SELECT toIntervalMinute(2) = toIntervalQuarter(120); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing hours');
SELECT toIntervalHour(48) > toIntervalHour(2);
SELECT toIntervalHour(48) >= toIntervalDay(2);
SELECT toIntervalHour(672) = toIntervalWeek(4);

SELECT toIntervalHour(48) < toIntervalHour(2);
SELECT toIntervalHour(48) < toIntervalDay(2);
SELECT toIntervalHour(672) != toIntervalWeek(4);

SELECT toIntervalHour(2) < toIntervalYear(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing days');
SELECT toIntervalDay(1) < toIntervalDay(23);
SELECT toIntervalDay(25) > toIntervalWeek(3);

SELECT toIntervalDay(1) > toIntervalDay(23);
SELECT toIntervalDay(25) < toIntervalWeek(3);

SELECT toIntervalDay(2) = toIntervalMonth(48); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing weeks');
SELECT toIntervalWeek(1) < toIntervalWeek(6);

SELECT toIntervalWeek(1) > toIntervalWeek(6);

SELECT toIntervalWeek(124) > toIntervalQuarter(8); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing months');
SELECT toIntervalMonth(1) < toIntervalMonth(3);
SELECT toIntervalMonth(124) > toIntervalQuarter(5);
SELECT toIntervalMonth(36) = toIntervalYear(3);

SELECT toIntervalMonth(1) > toIntervalMonth(3);
SELECT toIntervalMonth(124) < toIntervalQuarter(5);
SELECT toIntervalMonth(36) != toIntervalYear(3);

SELECT toIntervalMonth(6) = toIntervalMicrosecond(26); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing quarters');
SELECT toIntervalQuarter(5) > toIntervalQuarter(4);
SELECT toIntervalQuarter(20) = toIntervalYear(5);

SELECT toIntervalQuarter(5) < toIntervalQuarter(4);
SELECT toIntervalQuarter(20) != toIntervalYear(5);

SELECT toIntervalQuarter(2) = toIntervalNanosecond(6); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT('Comparing years');
SELECT toIntervalYear(1) < toIntervalYear(3);

SELECT toIntervalYear(1) > toIntervalYear(3);

SELECT toIntervalYear(2) = toIntervalSecond(8); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
