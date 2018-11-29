DROP TABLE IF EXISTS test.alter;
CREATE TABLE test.alter (`boolean_false` Nullable(String)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test.alter (`boolean_false`) VALUES (NULL), (''), ('123');
SELECT * FROM test.alter;
SELECT * FROM test.alter ORDER BY boolean_false NULLS LAST;

ALTER TABLE test.alter MODIFY COLUMN `boolean_false` Nullable(UInt8);
SELECT * FROM test.alter;
SELECT * FROM test.alter ORDER BY boolean_false NULLS LAST;

DROP TABLE test.alter;
