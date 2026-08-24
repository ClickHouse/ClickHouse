
DROP TABLE IF EXISTS 03443_data;

CREATE TABLE 03443_data
(
    id Int32,
    name String,
    INDEX idx_name name TYPE ngrambf_v1(1, 1024, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1
AS
SELECT 1, 'John' UNION ALL
SELECT 2, 'Ksenia' UNION ALL
SELECT 3, 'Alice';

SELECT '-- Without index';
SELECT name FROM 03443_data WHERE match(name, 'J|XYZ') SETTINGS use_skip_indexes = 0;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|J') SETTINGS use_skip_indexes = 0;
SELECT name FROM 03443_data WHERE match(name, '[J]|XYZ') SETTINGS use_skip_indexes = 0;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|[J]') SETTINGS use_skip_indexes = 0;

SELECT '-- With index';
SELECT name FROM 03443_data WHERE match(name, 'J|XYZ') SETTINGS use_skip_indexes = 1;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|J') SETTINGS use_skip_indexes = 1;
SELECT name FROM 03443_data WHERE match(name, '[J]|XYZ') SETTINGS use_skip_indexes = 1;
SELECT name FROM 03443_data WHERE match(name, 'XYZ|[J]') SETTINGS use_skip_indexes = 1;

SELECT '-- Assert selected granules';

SET parallel_replicas_local_plan = 1;

SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, 'J|XYZ')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;
SELECT '';
SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, 'XYZ|J')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;
SELECT '';
SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, '[J]|XYZ')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;
SELECT '';
SELECT trim(leading ' ' from explain) FROM (EXPLAIN indexes=1 SELECT name FROM 03443_data WHERE match(name, 'XYZ|[J]')) WHERE explain LIKE '%Granules: %' SETTINGS use_skip_indexes = 1;

DROP TABLE 03443_data;
