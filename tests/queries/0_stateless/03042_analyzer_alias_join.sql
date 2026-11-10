-- https://github.com/ClickHouse/ClickHouse/issues/14978
SET enable_analyzer=1;
CREATE TABLE test1(id UInt64, t1value UInt64) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE test2(id UInt64, t2value String) ENGINE=MergeTree ORDER BY tuple();

SELECT NULL AS t2value
FROM test1 t1
LEFT JOIN (
    SELECT id, t2value FROM test2
) t2
ON t1.id=t2.id
WHERE t2.t2value='test';

-- workaround should work too
SELECT NULL AS _svalue
FROM test1 t1
LEFT JOIN (
    SELECT id, t2value FROM test2
) t2
ON t1.id=t2.id
WHERE t2.t2value='test';
