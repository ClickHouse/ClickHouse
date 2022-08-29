DROP TABLE IF EXISTS test;
CREATE TABLE test (a UInt64, b String) ENGINE = MergeTree order by (a, b);

INSERT INTO test VALUES (1, 'Hello');
ALTER TABLE test MODIFY SETTING compress_marks = true, compress_primary_key = true;
INSERT INTO test VALUES (2, 'World');

SELECT * FROM test ORDER BY a;
OPTIMIZE TABLE test FINAL;
SELECT * FROM test ORDER BY a;

DROP TABLE test;
