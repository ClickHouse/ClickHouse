SELECT toDate('2018-06-21') % 234 = toInt16(toDate('2018-06-21')) % 234;
SELECT toDate('2018-06-21') % 23456 = toInt16(toDate('2018-06-21')) % 23456;
SELECT toDate('2018-06-21') % 12376 = toInt16(toDate('2018-06-21')) % 12376;
SELECT toDateTime('2018-06-21 12:12:12') % 234 = toInt32(toDateTime('2018-06-21 12:12:12')) % 234;
SELECT toDateTime('2018-06-21 12:12:12') % 23456 = toInt32(toDateTime('2018-06-21 12:12:12')) % 23456;
SELECT toDateTime('2018-06-21 12:12:12') % 12376 = toInt32(toDateTime('2018-06-21 12:12:12')) % 12376;

SELECT toDate('2018-06-21') % 234.8 = toInt16(toDate('2018-06-21')) % 234.8;
SELECT toDate('2018-06-21') % 23456.8 = toInt16(toDate('2018-06-21')) % 23456.8;
SELECT toDate('2018-06-21') % 12376.8 = toInt16(toDate('2018-06-21')) % 12376.8;
SELECT toDateTime('2018-06-21 12:12:12') % 234.8 = toInt32(toDateTime('2018-06-21 12:12:12')) % 234.8;
SELECT toDateTime('2018-06-21 12:12:12') % 23456.8 = toInt32(toDateTime('2018-06-21 12:12:12')) % 23456.8;
SELECT toDateTime('2018-06-21 12:12:12') % 12376.8 = toInt32(toDateTime('2018-06-21 12:12:12')) % 12376.8;
