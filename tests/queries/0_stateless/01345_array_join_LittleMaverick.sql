DROP TABLE IF EXISTS test;

CREATE TABLE test
(
  `id` Nullable(String),
  `status` Nullable(Enum8('NEW' = 0, 'CANCEL' = 1)),
  `nested.nestedType` Array(Nullable(String)),
  `partition` Date
) ENGINE = MergeTree() PARTITION BY partition
ORDER BY
  partition SETTINGS index_granularity = 8192;

INSERT INTO test VALUES ('1', 'NEW', array('a', 'b'), now());

SELECT
    status,
    count() AS all
FROM test ARRAY JOIN nested as nestedJoined
WHERE (status IN (
    SELECT status
    FROM test ARRAY JOIN nested as nestedJoined
    GROUP BY status 
    ORDER BY count() DESC 
    LIMIT 10)) AND (id IN ('1', '2'))
GROUP BY CUBE(status)
LIMIT 100;

DROP TABLE test;
