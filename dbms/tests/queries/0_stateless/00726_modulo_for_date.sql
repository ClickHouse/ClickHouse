SELECT toDate('21-06-2018') % 234 = toInt16(toDate('21-06-2018')) % 234;
SELECT toDate('21-06-2018') % 23456 = toInt16(toDate('21-06-2018')) % 23456;
SELECT toDate('21-06-2018') % 12376 = toInt16(toDate('21-06-2018')) % 12376;
SELECT toDate('21-06-2018 12:12:12') % 234 = toInt32(toDate('21-06-2018 12:12:12')) % 234;
SELECT toDate('21-06-2018 12:12:12') % 23456 = toInt32(toDate('21-06-2018 12:12:12')) % 23456;
SELECT toDate('21-06-2018 12:12:12') % 12376 = toInt32(toDate('21-06-2018 12:12:12')) % 12376;
