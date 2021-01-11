SELECT number % 4 AS k, groupArray(number), groupBitOr(number), groupBitAnd(number), groupBitXor(number) FROM (SELECT * FROM system.numbers LIMIT 20) GROUP BY k ORDER BY k;
