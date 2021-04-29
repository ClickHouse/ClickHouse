DROP TABLE IF EXISTS join;

CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);

INSERT INTO join VALUES (1, 'abc'), (2, 'def');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;

INSERT INTO join VALUES (6, 'ghi');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;

SELECT k, js1.s, join.s FROM (SELECT number AS k, number as s FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;
SELECT k, js1.s, join.s FROM (SELECT toUInt64(number / 3) AS k, sum(number) as s FROM numbers(10) GROUP BY toUInt64(number / 3) WITH TOTALS) js1 ANY LEFT JOIN join USING k;

-- JOIN ON not supported for storage join
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join ON js1.k == join.k; -- { serverError 48 }

DROP TABLE join;
