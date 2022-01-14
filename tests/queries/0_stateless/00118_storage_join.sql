DROP TABLE IF EXISTS t2;

CREATE TABLE t2 (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);

INSERT INTO t2 VALUES (1, 'abc'), (2, 'def');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN t2 USING k;

INSERT INTO t2 VALUES (6, 'ghi');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN t2 USING k;

SELECT k, js1.s, t2.s FROM (SELECT number AS k, number as s FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN t2 USING k;
SELECT k, t2.k, js1.s, t2.s FROM (SELECT number AS k, number as s FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN t2 USING k;

SELECT k, js1.s, t2.s FROM (SELECT toUInt64(number / 3) AS k, sum(number) as s FROM numbers(10) GROUP BY toUInt64(number / 3) WITH TOTALS) js1 ANY LEFT JOIN t2 USING k;

SELECT k, js1.s, t2.s FROM (SELECT number AS k, number AS s FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN t2 ON js1.k == t2.k;
SELECT k, t2.k, js1.s, t2.s FROM (SELECT number AS k, number AS s FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN t2 ON js1.k == t2.k;

DROP TABLE t2;
