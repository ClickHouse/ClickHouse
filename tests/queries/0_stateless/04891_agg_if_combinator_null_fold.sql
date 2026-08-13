-- A literal NULL argument folds before any combinator is applied. Issue #113763.
SELECT toTypeName(countIfOrNull(number, NULL)), countIfOrNull(number, NULL) FROM numbers(5) ORDER BY 1;
SELECT toTypeName(uniqIfOrNull(number, NULL)), uniqIfOrNull(number, NULL) FROM numbers(5) ORDER BY 1;
SELECT toTypeName(sumIfResample(0, 2, 1)(number, NULL, number % 2)), sumIfResample(0, 2, 1)(number, NULL, number % 2) FROM numbers(5) ORDER BY 1;
SELECT toTypeName(x) FROM (SELECT sumIfState(number, NULL) AS x FROM numbers(5)) ORDER BY 1;
