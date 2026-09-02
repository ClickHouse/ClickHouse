SELECT (number, nullIf(number % 3, 0), toString(nullIf(number % 2, 0))) AS tuple FROM system.numbers LIMIT 10 FORMAT PrettyCompactNoEscapes;
SELECT NULL AS x, tuple(NULL) AS y FORMAT PrettyCompactNoEscapes;
