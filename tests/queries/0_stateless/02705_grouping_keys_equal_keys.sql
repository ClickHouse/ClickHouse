SELECT count()
FROM numbers(2)
GROUP BY
GROUPING SETS (
  (number, number + 0, number + 1),
  (number % 1048576, number % -9223372036854775808),
  (number / 2, number / 2));
