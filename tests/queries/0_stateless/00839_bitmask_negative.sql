SELECT bitmaskToList(0);
SELECT bitmaskToArray(0);
SELECT bitmaskToList(7);
SELECT bitmaskToArray(7);
SELECT bitmaskToList(-1);
SELECT bitmaskToArray(-1);
SELECT bitmaskToList(-128);
SELECT bitmaskToArray(-128);

SELECT bitmaskToList(toInt64(0));
SELECT bitmaskToArray(toInt64(0));
SELECT bitmaskToList(toInt64(7));
SELECT bitmaskToArray(toInt64(7));
SELECT bitmaskToList(toInt64(-1));
SELECT bitmaskToArray(toInt64(-1));
SELECT bitmaskToList(toInt64(-128));
SELECT bitmaskToArray(toInt64(-128));
