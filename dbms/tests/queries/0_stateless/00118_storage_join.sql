DROP TABLE IF EXISTS test.join;

CREATE TABLE test.join (k UInt64, s String) ENGINE = Join(ANY, LEFT, k);

USE test;

INSERT INTO test.join VALUES (1, 'abc'), (2, 'def');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) ANY LEFT JOIN join USING k;

INSERT INTO test.join VALUES (6, 'ghi');
SELECT k, s FROM (SELECT number AS k FROM system.numbers LIMIT 10) ANY LEFT JOIN join USING k;

USE default;

DROP TABLE test.join;
