CREATE TABLE t1 (c0 Int32, PRIMARY KEY (c0)) ENGINE = MergeTree;
SELECT DISTINCT *
FROM
(
    SELECT DISTINCT
        cos(sign(exp(t1.c0))),
        -min2(pow(t1.c0, t1.c0), intDiv(t1.c0, t1.c0)),
        t1.c0,
        t1.c0,
        erf(abs(-t1.c0))
    FROM t1
    WHERE t1.c0 > 0
    UNION ALL
    SELECT DISTINCT
        cos(sign(exp(t1.c0))),
        -min2(pow(t1.c0, t1.c0), intDiv(t1.c0, t1.c0)),
        t1.c0,
        t1.c0,
        erf(abs(-t1.c0))
    FROM t1
    WHERE NOT (t1.c0 > 0)
    UNION ALL
    SELECT DISTINCT
        cos(sign(exp(t1.c0))),
        -min2(pow(t1.c0, t1.c0), intDiv(t1.c0, t1.c0)),
        t1.c0,
        t1.c0,
        erf(abs(-t1.c0))
    FROM t1
    WHERE t1.c0 > (0 IS NULL)
);

