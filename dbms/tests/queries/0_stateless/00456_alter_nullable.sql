DROP TABLE IF EXISTS test.nullable_alter;
CREATE TABLE test.nullable_alter (d Date DEFAULT '2000-01-01', x String) ENGINE = MergeTree(d, d, 1);

INSERT INTO test.nullable_alter (x) VALUES ('Hello'), ('World');
SELECT x FROM test.nullable_alter ORDER BY x;

ALTER TABLE test.nullable_alter MODIFY COLUMN x Nullable(String);
SELECT x FROM test.nullable_alter ORDER BY x;

INSERT INTO test.nullable_alter (x) VALUES ('xyz'), (NULL);
SELECT x FROM test.nullable_alter ORDER BY x NULLS FIRST;

ALTER TABLE test.nullable_alter MODIFY COLUMN x Nullable(FixedString(5));
SELECT x FROM test.nullable_alter ORDER BY x NULLS FIRST;

DROP TABLE test.nullable_alter;
