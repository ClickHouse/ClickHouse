SELECT toDate('2018-06-21') % 234 = toUInt16(toDate('2018-06-21')) % 234;
SELECT toDate('2018-06-21') % 23456 = toUInt16(toDate('2018-06-21')) % 23456;
SELECT toDate('2018-06-21') % 12376 = toUInt16(toDate('2018-06-21')) % 12376;
SELECT toDateTime('2018-06-21 12:12:12') % 234 = toUInt32(toDateTime('2018-06-21 12:12:12')) % 234;
SELECT toDateTime('2018-06-21 12:12:12') % 23456 = toUInt32(toDateTime('2018-06-21 12:12:12')) % 23456;
SELECT toDateTime('2018-06-21 12:12:12') % 12376 = toUInt32(toDateTime('2018-06-21 12:12:12')) % 12376;

SELECT toDate('2018-06-21') % 234.8 = toUInt16(toDate('2018-06-21')) % 234.8;
SELECT toDate('2018-06-21') % 23456.8 = toUInt16(toDate('2018-06-21')) % 23456.8;
SELECT toDate('2018-06-21') % 12376.8 = toUInt16(toDate('2018-06-21')) % 12376.8;
SELECT toDateTime('2018-06-21 12:12:12') % 234.8 = toUInt32(toDateTime('2018-06-21 12:12:12')) % 234.8;
SELECT toDateTime('2018-06-21 12:12:12') % 23456.8 = toUInt32(toDateTime('2018-06-21 12:12:12')) % 23456.8;
SELECT toDateTime('2018-06-21 12:12:12') % 12376.8 = toUInt32(toDateTime('2018-06-21 12:12:12')) % 12376.8;
