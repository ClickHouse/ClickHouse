SELECT number % 100 AS k, sumArray(emptyArrayUInt8()) AS v FROM numbers(10) GROUP BY k;
