SELECT number % 4 AS k, groupArray(number), groupBitOr(number), groupBitAnd(number), groupBitXor(number) FROM (SELECT * FROM system.numbers LIMIT 20) GROUP BY k ORDER BY k;
SELECT number % 4 AS k, groupArray(-number), groupBitOr(-number), groupBitAnd(-number), groupBitXor(-number) FROM (SELECT * FROM system.numbers LIMIT 20) GROUP BY k ORDER BY k;
SELECT number % 4 AS k, groupArray(number-10), groupBitOr(number-10), groupBitAnd(number-10), groupBitXor(number-10) FROM (SELECT * FROM system.numbers LIMIT 20) GROUP BY k ORDER BY k;
