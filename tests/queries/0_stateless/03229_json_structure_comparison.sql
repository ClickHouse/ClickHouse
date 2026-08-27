SET enable_json_type=1;

DROP TABLE IF EXISTS test_new_json_type;

CREATE TABLE test_new_json_type(id UInt32, data JSON, version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY id;

INSERT INTO test_new_json_type format JSONEachRow
{"id":1,"data":{"foo1":"bar"},"version":1}
{"id":2,"data":{"foo2":"bar"},"version":1}
{"id":3,"data":{"foo2":"bar"},"version":1}
;

SELECT
    a.data,
    b.data
FROM test_new_json_type AS a
INNER JOIN test_new_json_type AS b ON a.id = b.id
ORDER BY id;

DROP TABLE test_new_json_type;
