SELECT coalesce(), coalesce(NULL), coalesce(NULL, NULL),
    coalesce(1), coalesce(1, NULL), coalesce(NULL, 1), coalesce(NULL, 1, NULL);

SELECT COALESCE(), COALESCE(NULL), COALESCE(1, NULL);

SELECT coalesce(number % 2 = 0 ? number : NULL, number % 3 = 0 ? number : NULL, number % 5 = 0 ? number : NULL) AS res, toTypeName(res) FROM system.numbers LIMIT 15;
SELECT coalesce(number % 2 = 0 ? number : NULL, number % 3 = 0 ? number : NULL, number) AS res, toTypeName(res) FROM system.numbers LIMIT 15;
SELECT coalesce(number % 2 = 0 ? number : NULL, number % 3 = 0 ? number : NULL, 100) AS res, toTypeName(res) FROM system.numbers LIMIT 15;
