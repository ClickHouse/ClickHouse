DROP TABLE IF EXISTS test.smallest_json;

CREATE TABLE test.generics_type_test(t Date, json SmallestJSON) ENGINE = MergeTree PARTITION BY t ORDER BY t;

-- INSERT INTO test.generics_type_test VALUES('2019-01-01', 'true'), ('2019-01-01', '1'), ('2019-01-01', '-1'), ('2019-01-01', '1.0'), ('2019-01-01', '"zhangsan"'), ('2019-01-01', '{"name": "zhangsan"}');

DROP TABLE IF EXISTS test.smallest_json;
