DROP TABLE IF EXISTS data_02771;


CREATE TABLE data_02771
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1,
    INDEX y_idx y TYPE minmax GRANULARITY 1,
    INDEX xy_idx (x,y) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO data_02771 VALUES (1, 2, 3);

SELECT * FROM data_02771;
SELECT * FROM data_02771 SETTINGS ignore_data_skipping_indices=''; -- { serverError CANNOT_PARSE_TEXT }
SELECT * FROM data_02771 SETTINGS ignore_data_skipping_indices='x_idx';
SELECT * FROM data_02771 SETTINGS ignore_data_skipping_indices='na_idx';

SELECT * FROM data_02771 WHERE x = 1 AND y = 1 SETTINGS ignore_data_skipping_indices='xy_idx',force_data_skipping_indices='xy_idx' ; -- { serverError INDEX_NOT_USED }
SELECT * FROM data_02771 WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';

SET enable_analyzer = 0;

SELECT * from ( EXPLAIN indexes = 1 SELECT * FROM data_02771 WHERE x = 1 AND y = 2 ) WHERE explain NOT LIKE '%Expression%' AND explain NOT LIKE '%Filter%';
SELECT * from ( EXPLAIN indexes = 1 SELECT * FROM data_02771 WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx' ) WHERE explain NOT LIKE '%Expression%' AND explain NOT LIKE '%Filter%';

SET enable_analyzer = 1;

SELECT * from ( EXPLAIN indexes = 1 SELECT * FROM data_02771 WHERE x = 1 AND y = 2 ) WHERE explain NOT LIKE '%Expression%' AND explain NOT LIKE '%Filter%';
SELECT * from ( EXPLAIN indexes = 1 SELECT * FROM data_02771 WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx' ) WHERE explain NOT LIKE '%Expression%' AND explain NOT LIKE '%Filter%';

DROP TABLE data_02771;
