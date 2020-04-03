DROP TABLE IF EXISTS join;

CREATE TABLE join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);

INSERT INTO join VALUES (1, 'abc'), (2, 'def');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;

INSERT INTO join VALUES (6, 'ghi');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;

DROP TABLE join;
