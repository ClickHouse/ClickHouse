DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test(day Date, predicate UInt64, non_nullable_column JSONB, nullable_column Nullable(JSONB)) ENGINE = MergeTree PARTITION BY day ORDER BY day;

-- TODO: INSERT INTO test.test VALUES('2019-01-01', 0, null, null);
INSERT INTO test.test VALUES('2019-01-01', 0, 'null', 'null'); -- { clientError 485 }
INSERT INTO test.test VALUES('2019-01-01', 0, '[null]', '[null]'); -- { clientError 48 }

INSERT INTO test.test VALUES('2019-01-01', 1, 'true', 'null');
INSERT INTO test.test VALUES('2019-01-01', 1, 'false', 'true');

INSERT INTO test.test VALUES('2019-01-01', 2, '2147483648', '-2147483648');
INSERT INTO test.test VALUES('2019-01-01', 2, '4294967296', '-4294967296');
INSERT INTO test.test VALUES('2019-01-01', 2, '9223372036854775807', '-9223372036854775807');

INSERT INTO test.test VALUES('2019-01-01', 3, '"\\n\\u0000"', '"\\u263a"');
INSERT INTO test.test VALUES('2019-01-01', 4, '{"a": "hello", "b": "world"}', '{"a": "hello", "b": "world"}');

SELECT * FROM test.test;
SELECT * FROM test.test WHERE predicate = 1;
SELECT * FROM test.test ORDER BY predicate DESC;
SELECT * FROM test.test ORDER BY predicate DESC LIMIT 1;

DROP TABLE IF EXISTS test.test;
