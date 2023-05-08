WITH 18 AS precision, toUInt256(-1) AS int, toUInt256(toFloat64(toUInt256(-1))) AS converted SELECT substring(toString(int), 1, precision) = substring(toString(converted), 1, precision) AS res;
