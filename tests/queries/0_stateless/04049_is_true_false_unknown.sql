SELECT 'is_true', 1 WHERE true IS TRUE;
SELECT 'is_false', 1 WHERE false IS FALSE;
SELECT 'is_unknown', 1 WHERE NULL IS UNKNOWN;
SELECT 'null_is_true', 1 WHERE NULL IS TRUE;
SELECT 'null_is_not_unknown', 1 WHERE NULL IS NOT UNKNOWN;
SELECT 'is_not_false', 1 WHERE true IS NOT FALSE;
SELECT 'false_is_not_true', 1 WHERE false IS NOT TRUE;
SELECT 'null_is_not_true', 1 WHERE NULL IS NOT TRUE;
SELECT 'true_is_not_unknown', 1 WHERE true IS NOT UNKNOWN;

SELECT 'expr_true', 1 WHERE (1 = 1) IS TRUE;
SELECT 'expr_false', 1 WHERE (1 = 0) IS FALSE;
SELECT 'expr_unknown', 1 WHERE (NULL = 1) IS UNKNOWN;

DROP TABLE IF EXISTS bool_predicates;
CREATE TABLE bool_predicates (x Nullable(Bool)) ENGINE = Memory;
INSERT INTO bool_predicates VALUES (true), (false), (NULL);

SELECT
    ifNull(toInt8(x), -1) AS val,
    toUInt8(x IS TRUE),
    toUInt8(x IS FALSE),
    toUInt8(x IS UNKNOWN),
    toUInt8(x IS NOT TRUE),
    toUInt8(x IS NOT FALSE),
    toUInt8(x IS NOT UNKNOWN)
FROM bool_predicates
ORDER BY isNull(x), x;

DROP TABLE bool_predicates;
