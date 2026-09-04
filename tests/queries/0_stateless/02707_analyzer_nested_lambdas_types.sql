SELECT
    range(1),
    arrayMap(x -> arrayMap(x -> x, range(x)), [1])
SETTINGS enable_analyzer = 0;

SELECT
    range(1),
    arrayMap(x -> arrayMap(x -> x, range(x)), [1])
SETTINGS enable_analyzer = 1;

SELECT
    range(1),
    arrayMap(x -> arrayMap(x -> 1, range(x)), [1])
SETTINGS enable_analyzer = 0;

SELECT
    range(1),
    arrayMap(x -> arrayMap(x -> 1, range(x)), [1])
SETTINGS enable_analyzer = 1;

SELECT
    range(1),
    arrayMap(x -> arrayMap(y -> 1, range(x)), [1])
SETTINGS enable_analyzer = 1;
