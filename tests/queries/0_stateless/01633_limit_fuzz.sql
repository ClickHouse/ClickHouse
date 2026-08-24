SELECT number, 1 AS k FROM numbers(100000) ORDER BY k, number LIMIT 1025, 1023 FORMAT Values;
