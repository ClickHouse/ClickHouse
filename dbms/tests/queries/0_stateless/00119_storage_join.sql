DROP TABLE IF EXISTS test.join;

CREATE TABLE test.join (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, LEFT, k);

USE test;

INSERT INTO test.join VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO test.join (k, s) VALUES (3, 'ghi');
INSERT INTO test.join (x, k) VALUES ([3, 4, 5], 4);

SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) ANY LEFT JOIN join USING k;
SELECT s, x FROM (SELECT number AS k FROM system.numbers LIMIT 10) ANY LEFT JOIN join USING k;
SELECT x, s, k FROM (SELECT number AS k FROM system.numbers LIMIT 10) ANY LEFT JOIN join USING k;
SELECT 1, x, 2, s, 3, k, 4 FROM (SELECT number AS k FROM system.numbers LIMIT 10) ANY LEFT JOIN join USING k;

USE default;

DROP TABLE test.join;
