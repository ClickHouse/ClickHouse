SET allow_experimental_low_cardinality_type = 1;

DROP TABLE IF EXISTS test.table1;
DROP TABLE IF EXISTS test.table2;

CREATE TABLE test.table1
(
dt Date,
id Int32,
arr Array(LowCardinality(String))
) ENGINE = MergeTree PARTITION BY toMonday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

CREATE TABLE test.table2
(
dt Date,
id Int32,
arr Array(LowCardinality(String))
) ENGINE = MergeTree PARTITION BY toMonday(dt)
ORDER BY (dt, id) SETTINGS index_granularity = 8192;

INSERT INTO test.table1 (dt, id, arr) VALUES ('2019-01-14', 1, ['aaa']);
INSERT INTO test.table2 (dt, id, arr) VALUES ('2019-01-14', 1, ['aaa','bbb','ccc']);

SET max_threads = 1;

SELECT dt, id, groupArrayArray(arr)
FROM (
	SELECT dt, id, arr FROM test.table1
	WHERE dt = '2019-01-14' AND id = 1
	UNION ALL
	SELECT dt, id, arr FROM test.table2
	WHERE dt = '2019-01-14' AND id = 1
)
GROUP BY dt, id;
