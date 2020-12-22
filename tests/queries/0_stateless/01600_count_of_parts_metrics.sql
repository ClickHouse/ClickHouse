
DROP TABLE IF EXISTS test_table SYNC;
CREATE TABLE test_table(data Date) ENGINE = MergeTree  PARTITION BY toYear(data) ORDER BY data;

INSERT INTO test_table VALUES ('1992-01-01');

SELECT COUNT(1)
FROM
(SELECT
    SUM(IF(metric = 'Parts', value, 0)) AS Parts,
    SUM(IF(metric = 'PartsActive', value, 0)) AS PartsActive,
    SUM(IF(metric = 'PartsInactive', value, 0)) AS PartsInactive
FROM system.metrics) as a INNER JOIN
(SELECT
    toInt64(SUM(1)) AS Parts,
    toInt64(SUM(IF(active = 1, 1, 0))) AS PartsActive,
    toInt64(SUM(IF(active = 0, 1, 0))) AS PartsInactive
FROM system.parts
) as b USING (Parts,PartsActive,PartsInactive);

INSERT INTO test_table VALUES ('1992-01-02');

SELECT COUNT(1)
FROM
(SELECT
    SUM(IF(metric = 'Parts', value, 0)) AS Parts,
    SUM(IF(metric = 'PartsActive', value, 0)) AS PartsActive,
    SUM(IF(metric = 'PartsInactive', value, 0)) AS PartsInactive
FROM system.metrics) as a INNER JOIN
(SELECT
    toInt64(SUM(1)) AS Parts,
    toInt64(SUM(IF(active = 1, 1, 0))) AS PartsActive,
    toInt64(SUM(IF(active = 0, 1, 0))) AS PartsInactive
FROM system.parts
) as b USING (Parts,PartsActive,PartsInactive);

OPTIMIZE TABLE test_table FINAL;


SELECT COUNT(1)
FROM
(SELECT
    SUM(IF(metric = 'Parts', value, 0)) AS Parts,
    SUM(IF(metric = 'PartsActive', value, 0)) AS PartsActive,
    SUM(IF(metric = 'PartsInactive', value, 0)) AS PartsInactive
FROM system.metrics) as a INNER JOIN
(SELECT
    toInt64(SUM(1)) AS Parts,
    toInt64(SUM(IF(active = 1, 1, 0))) AS PartsActive,
    toInt64(SUM(IF(active = 0, 1, 0))) AS PartsInactive
FROM system.parts
) as b USING (Parts,PartsActive,PartsInactive);

DROP TABLE test_table SYNC;
