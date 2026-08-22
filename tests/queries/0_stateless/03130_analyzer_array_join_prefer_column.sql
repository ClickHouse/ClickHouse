DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (`id` UInt64, `value` String, `value_array` Array(UInt64)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO test_table VALUES (0, 'aaa', [0]), (1, 'bbb', [1]), (2, 'ccc', [2]);


SELECT materialize(id), toTypeName(id)
FROM ( SELECT 'aaa' ) AS subquery
ARRAY JOIN [0] AS id
INNER JOIN test_table
USING (id)
;

SELECT materialize(id), toTypeName(id)
FROM ( SELECT 'aaa' ) AS subquery
ARRAY JOIN [0] AS id
INNER JOIN test_table
USING (id)
SETTINGS prefer_column_name_to_alias = 1
;
