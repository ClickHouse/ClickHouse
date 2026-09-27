-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: output of explain is different

SET enable_analyzer=1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (
    x String,
    y Tuple(s String),
    z String,
    PROJECTION sorted_by_company_name (
        SELECT *
        ORDER BY x
    )
) ENGINE = MergeTree()
ORDER BY z SETTINGS index_granularity=1;

INSERT INTO test SELECT 'x_' || number, tuple('y_' || number), 'z_' || number FROM numbers(5);
EXPLAIN indexes=1 SELECT count() FROM test WHERE x = 'x_1' and y.s = 'y_1';
SELECT count() FROM test WHERE x = 'x_1' and y.s = 'y_1';
SELECT x, y.s, z FROM test WHERE x = 'x_1' and y.s = 'y_1';
DROP TABLE test;
