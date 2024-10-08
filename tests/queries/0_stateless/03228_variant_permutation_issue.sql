SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS test_new_json_type;
CREATE TABLE test_new_json_type(id UInt32, data JSON, version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY id;
INSERT INTO test_new_json_type format JSONEachRow
{"id":1,"data":{"foo1":"bar"},"version":1}
{"id":2,"data":{"foo2":"bar"},"version":1}
{"id":3,"data":{"foo2":"bar"},"version":1}
;

SELECT * FROM test_new_json_type FINAL WHERE data.foo2 is not null ORDER BY id;

INSERT INTO test_new_json_type SELECT id, '{"foo2":"baz"}' AS _data, version+1 AS _version FROM test_new_json_type where id=2;

SELECT * FROM test_new_json_type FINAL WHERE data.foo2 is not null ORDER BY id;

DROP TABLE test_new_json_type;

CREATE TABLE test_new_json_type(id Nullable(UInt32), data JSON, version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY id settings allow_nullable_key=1;
INSERT INTO test_new_json_type format JSONEachRow
{"id":1,"data":{"foo1":"bar"},"version":1}
{"id":2,"data":{"foo2":"bar"},"version":1}
{"id":3,"data":{"foo2":"bar"},"version":1}
;

SELECT * FROM test_new_json_type FINAL WHERE data.foo2 is not null ORDER BY id;

INSERT INTO test_new_json_type SELECT id, '{"foo2":"baz"}' AS _data, version+1 AS _version FROM test_new_json_type where id=2;

SELECT * FROM test_new_json_type FINAL PREWHERE data.foo2 IS NOT NULL WHERE data.foo2 IS NOT NULL ORDER BY id ASC NULLS FIRST;

DROP TABLE test_new_json_type;

