-- Correctness of the vectorized batch paths of the variance family:
-- the batch path (aggregation without keys) must match the scalar path
-- (forced via State/Merge with GROUP BY, which uses per-row add). On small
-- integer inputs all moment sums are exact in Float64, so results match exactly.

SELECT 'varPop int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT varPop(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT varPopMerge(s) FROM (SELECT varPopState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT varPopIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT varPopIfMerge(s) FROM (SELECT varPopIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT varPop(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT varPopMerge(s) FROM (SELECT varPopState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT varPopIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT varPopIfMerge(s) FROM (SELECT varPopIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'varPop f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT varPop(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT varPopMerge(s) FROM (SELECT varPopState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT varPopIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT varPopIfMerge(s) FROM (SELECT varPopIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT varPopIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT varPopIfMerge(s) FROM (SELECT varPopIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'varSamp int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT varSamp(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT varSampMerge(s) FROM (SELECT varSampState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT varSampIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT varSampIfMerge(s) FROM (SELECT varSampIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT varSamp(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT varSampMerge(s) FROM (SELECT varSampState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT varSampIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT varSampIfMerge(s) FROM (SELECT varSampIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'varSamp f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT varSamp(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT varSampMerge(s) FROM (SELECT varSampState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT varSampIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT varSampIfMerge(s) FROM (SELECT varSampIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT varSampIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT varSampIfMerge(s) FROM (SELECT varSampIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'stddevPop int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT stddevPop(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT stddevPopMerge(s) FROM (SELECT stddevPopState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT stddevPopIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT stddevPopIfMerge(s) FROM (SELECT stddevPopIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT stddevPop(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT stddevPopMerge(s) FROM (SELECT stddevPopState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT stddevPopIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT stddevPopIfMerge(s) FROM (SELECT stddevPopIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'stddevPop f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT stddevPop(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT stddevPopMerge(s) FROM (SELECT stddevPopState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT stddevPopIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT stddevPopIfMerge(s) FROM (SELECT stddevPopIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT stddevPopIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT stddevPopIfMerge(s) FROM (SELECT stddevPopIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'stddevSamp int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT stddevSamp(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT stddevSampMerge(s) FROM (SELECT stddevSampState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT stddevSampIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT stddevSampIfMerge(s) FROM (SELECT stddevSampIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT stddevSamp(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT stddevSampMerge(s) FROM (SELECT stddevSampState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT stddevSampIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT stddevSampIfMerge(s) FROM (SELECT stddevSampIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'stddevSamp f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT stddevSamp(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT stddevSampMerge(s) FROM (SELECT stddevSampState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT stddevSampIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT stddevSampIfMerge(s) FROM (SELECT stddevSampIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT stddevSampIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT stddevSampIfMerge(s) FROM (SELECT stddevSampIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'skewPop int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT skewPop(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT skewPopMerge(s) FROM (SELECT skewPopState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT skewPopIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT skewPopIfMerge(s) FROM (SELECT skewPopIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT skewPop(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT skewPopMerge(s) FROM (SELECT skewPopState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT skewPopIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT skewPopIfMerge(s) FROM (SELECT skewPopIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'skewPop f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT skewPop(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT skewPopMerge(s) FROM (SELECT skewPopState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT skewPopIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT skewPopIfMerge(s) FROM (SELECT skewPopIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT skewPopIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT skewPopIfMerge(s) FROM (SELECT skewPopIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'skewSamp int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT skewSamp(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT skewSampMerge(s) FROM (SELECT skewSampState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT skewSampIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT skewSampIfMerge(s) FROM (SELECT skewSampIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT skewSamp(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT skewSampMerge(s) FROM (SELECT skewSampState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT skewSampIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT skewSampIfMerge(s) FROM (SELECT skewSampIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'skewSamp f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT skewSamp(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT skewSampMerge(s) FROM (SELECT skewSampState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT skewSampIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT skewSampIfMerge(s) FROM (SELECT skewSampIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT skewSampIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT skewSampIfMerge(s) FROM (SELECT skewSampIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

-- Unlike the other moments, kurtosis accumulates a sum of 4th powers, so the values are kept
-- under 100: with `% 1000` the sum reaches ~1.4e16 and exceeds 2^53, making it inexact and thus
-- dependent on summation order (block size, thread count), which is randomized across CI runs.
SELECT 'kurtPop int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT kurtPop(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT kurtPopMerge(s) FROM (SELECT kurtPopState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT kurtPopIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT kurtPopIfMerge(s) FROM (SELECT kurtPopIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT kurtPop(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT kurtPopMerge(s) FROM (SELECT kurtPopState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT kurtPopIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT kurtPopIfMerge(s) FROM (SELECT kurtPopIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'kurtPop f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT kurtPop(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT kurtPopMerge(s) FROM (SELECT kurtPopState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT kurtPopIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT kurtPopIfMerge(s) FROM (SELECT kurtPopIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT kurtPopIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT kurtPopIfMerge(s) FROM (SELECT kurtPopIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

-- Values kept under 100 so that the sum of 4th powers stays exact, see `kurtPop int exact` above.
SELECT 'kurtSamp int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT kurtSamp(v) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT kurtSampMerge(s) FROM (SELECT kurtSampState(v) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT kurtSampIf(v, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT kurtSampIfMerge(s) FROM (SELECT kurtSampIfState(v, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT kurtSamp(vn) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT kurtSampMerge(s) FROM (SELECT kurtSampState(vn) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT kurtSampIf(vn, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT kurtSampIfMerge(s) FROM (SELECT kurtSampIfState(vn, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 100) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'kurtSamp f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT kurtSamp(v) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT kurtSampMerge(s) FROM (SELECT kurtSampState(v) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT kurtSampIf(v, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT kurtSampIfMerge(s) FROM (SELECT kurtSampIfState(v, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT kurtSampIf(vn, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT kurtSampIfMerge(s) FROM (SELECT kurtSampIfState(vn, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'covarPop int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT covarPop(v, w) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT covarPopMerge(s) FROM (SELECT covarPopState(v, w) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT covarPopIf(v, w, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT covarPopIfMerge(s) FROM (SELECT covarPopIfState(v, w, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT covarPop(vn, w) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT covarPopMerge(s) FROM (SELECT covarPopState(vn, w) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT covarPopIf(vn, w, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT covarPopIfMerge(s) FROM (SELECT covarPopIfState(vn, w, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'covarPop f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT covarPop(v, w) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT covarPopMerge(s) FROM (SELECT covarPopState(v, w) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT covarPopIf(v, w, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT covarPopIfMerge(s) FROM (SELECT covarPopIfState(v, w, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT covarPopIf(vn, w, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT covarPopIfMerge(s) FROM (SELECT covarPopIfState(vn, w, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'covarSamp int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT covarSamp(v, w) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT covarSampMerge(s) FROM (SELECT covarSampState(v, w) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT covarSampIf(v, w, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT covarSampIfMerge(s) FROM (SELECT covarSampIfState(v, w, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT covarSamp(vn, w) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT covarSampMerge(s) FROM (SELECT covarSampState(vn, w) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT covarSampIf(vn, w, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT covarSampIfMerge(s) FROM (SELECT covarSampIfState(vn, w, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'covarSamp f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT covarSamp(v, w) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT covarSampMerge(s) FROM (SELECT covarSampState(v, w) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT covarSampIf(v, w, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT covarSampIfMerge(s) FROM (SELECT covarSampIfState(v, w, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT covarSampIf(vn, w, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT covarSampIfMerge(s) FROM (SELECT covarSampIfState(vn, w, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'corr int exact', d0 = r0, d1 = r1, d2 = r2, d3 = r3
FROM
(
    SELECT
        (SELECT corr(v, w) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT corrMerge(s) FROM (SELECT corrState(v, w) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT corrIf(v, w, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT corrIfMerge(s) FROM (SELECT corrIfState(v, w, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT corr(vn, w) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT corrMerge(s) FROM (SELECT corrState(vn, w) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2,
        (SELECT corrIf(vn, w, c) FROM (SELECT toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d3,
        (SELECT corrIfMerge(s) FROM (SELECT corrIfState(vn, w, c) AS s FROM (SELECT number, toUInt32(cityHash64(number, 1) % 1000) AS v, toInt64(cityHash64(number, 2) % 1000) - 500 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r3
);
SELECT 'corr f64 approx', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-9 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT corr(v, w) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT corrMerge(s) FROM (SELECT corrState(v, w) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT corrIf(v, w, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT corrIfMerge(s) FROM (SELECT corrIfState(v, w, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1,
        (SELECT corrIf(vn, w, c) FROM (SELECT toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d2,
        (SELECT corrIfMerge(s) FROM (SELECT corrIfState(vn, w, c) AS s FROM (SELECT number, toFloat64(cityHash64(number, 1) % 1000) / 8 AS v, toFloat64(toInt64(cityHash64(number, 2) % 1000) - 500) / 4 AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r2
);

SELECT 'f32 approx', abs(d0 - r0) <= 1e-3 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-3 * greatest(abs(d1), abs(r1), 1e-30)
FROM
(
    SELECT
        (SELECT varSamp(v) FROM (SELECT toFloat32(cityHash64(number, 1) % 1000) AS v, toFloat32(toInt64(cityHash64(number, 2) % 1000) - 500) AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d0,
        (SELECT varSampMerge(s) FROM (SELECT varSampState(v) AS s FROM (SELECT number, toFloat32(cityHash64(number, 1) % 1000) AS v, toFloat32(toInt64(cityHash64(number, 2) % 1000) - 500) AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0,
        (SELECT varSampIf(v, c) FROM (SELECT toFloat32(cityHash64(number, 1) % 1000) AS v, toFloat32(toInt64(cityHash64(number, 2) % 1000) - 500) AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000))) AS d1,
        (SELECT varSampIfMerge(s) FROM (SELECT varSampIfState(v, c) AS s FROM (SELECT number, toFloat32(cityHash64(number, 1) % 1000) AS v, toFloat32(toInt64(cityHash64(number, 2) % 1000) - 500) AS w, cityHash64(number, 42) % 2 = 0 AS c, if(cityHash64(number, 7) % 4 = 0, NULL, v) AS vn FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r1
);

-- NaN/Inf values in rows excluded by the -If flag or the null map must not poison the result.
SELECT 'nan-safety If', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30), abs(d1 - r1) <= 1e-9 * greatest(abs(d1), abs(r1), 1e-30), abs(d2 - r2) <= 1e-6 * greatest(abs(d2), abs(r2), 1e-30)
FROM
(
    SELECT
        (SELECT varSampIf(v, c) FROM (SELECT number, if(number % 2 = 0, nan, toFloat64(number % 1000)) AS v, number % 2 = 1 AS c FROM numbers(70000))) AS d0,
        (SELECT varSamp(v) FROM (SELECT number, toFloat64(number % 1000) AS v FROM numbers(70000) WHERE number % 2 = 1)) AS r0,
        (SELECT kurtSampIf(v, c) FROM (SELECT number, if(number % 2 = 0, nan, toFloat64(number % 1000)) AS v, number % 2 = 1 AS c FROM numbers(70000))) AS d1,
        (SELECT kurtSamp(v) FROM (SELECT number, toFloat64(number % 1000) AS v FROM numbers(70000) WHERE number % 2 = 1)) AS r1,
        (SELECT corrIf(v, v + 1, c) FROM (SELECT number, if(number % 2 = 0, nan, toFloat64(number % 1000)) AS v, number % 2 = 1 AS c FROM numbers(70000))) AS d2,
        (SELECT corr(v, v + 1) FROM (SELECT number, toFloat64(number % 1000) AS v FROM numbers(70000) WHERE number % 2 = 1)) AS r2
);
SELECT 'inf-safety If', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30)
FROM
(
    SELECT
        (SELECT varSampIf(v, c) FROM (SELECT number, if(number % 2 = 0, inf, toFloat64(number % 1000)) AS v, number % 2 = 1 AS c FROM numbers(70000))) AS d0,
        (SELECT varSamp(v) FROM (SELECT number, toFloat64(number % 1000) AS v FROM numbers(70000) WHERE number % 2 = 1)) AS r0
);
SELECT 'nan-safety Null', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30)
FROM
(
    SELECT
        (SELECT varSamp(v) FROM (SELECT number, if(number % 2 = 0, nan, toFloat64(number % 1000)) AS vp, if(number % 2 = 0, NULL, vp) AS v FROM numbers(70000))) AS d0,
        (SELECT varSamp(v) FROM (SELECT number, toFloat64(number % 1000) AS v FROM numbers(70000) WHERE number % 2 = 1)) AS r0
);

-- Empty selection and all-false flags must give NaN (no rows accumulated).
SELECT 'empty', isNaN(varSamp(v)), isNaN(kurtPop(v)), isNaN(corr(v, v)) FROM (SELECT toFloat64(number) AS v FROM numbers(100) WHERE number > 100);
SELECT 'all-false', isNaN(varSampIf(v, c0)), isNaN(skewPopIf(v, c0)), isNaN(covarSampIf(v, v, c0)) FROM (SELECT toFloat64(number) AS v, number > 100 AS c0 FROM numbers(100));

SELECT 'bf16 exact', d0 = r0
FROM
(
    SELECT
        (SELECT varSamp(v) FROM (SELECT toBFloat16(cityHash64(number, 1) % 256) AS v FROM numbers(70000))) AS d0,
        (SELECT varSampMerge(s) FROM (SELECT varSampState(v) AS s FROM (SELECT number, toBFloat16(cityHash64(number, 1) % 256) AS v FROM numbers(70000)) GROUP BY cityHash64(number, 99) % 97)) AS r0
);

-- Decimal inputs use the generic per-row path; sanity-check against the integer result.
SELECT 'decimal', abs(d0 - r0) <= 1e-9 * greatest(abs(d0), abs(r0), 1e-30)
FROM
(
    SELECT
        (SELECT varSampIf(toDecimal64(number % 1000, 3), (cityHash64(number, 42) % 2) = 0) FROM numbers(70000)) AS d0,
        (SELECT varSampIf(number % 1000, (cityHash64(number, 42) % 2) = 0) FROM numbers(70000)) AS r0
);
