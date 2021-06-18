DROP TABLE IF EXISTS join;

CREATE TABLE join (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, LEFT, k);

INSERT INTO join VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join (k, s) VALUES (3, 'ghi');
INSERT INTO join (x, k) VALUES ([3, 4, 5], 4);

SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;
SELECT s, x FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;
SELECT x, s, k FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;
SELECT 1, x, 2, s, 3, k, 4 FROM (SELECT number AS k FROM system.numbers LIMIT 10) js1 ANY LEFT JOIN join USING k;

DROP TABLE join;
