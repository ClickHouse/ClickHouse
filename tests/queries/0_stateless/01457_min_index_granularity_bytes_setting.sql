DROP TABLE IF EXISTS invalid_min_index_granularity_bytes_setting;

SELECT '--- CREATE TABLE with INVALID index_granularity_bytes i.e.index_granularity_bytes < min_index_granularity_bytes ---';
CREATE TABLE invalid_min_index_granularity_bytes_setting
(
  id UInt64,
  value String
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 1, min_index_granularity_bytes = 1024; -- should result in exception

SELECT '--- INSERT INTO TABLE invalid_min_index_granularity_bytes_setting ---';

INSERT INTO invalid_min_index_granularity_bytes_setting SELECT number, concat('xxxxxxxxxx', toString(number)) FROM numbers(1000,1000); -- should result in exception

DROP TABLE IF EXISTS invalid_min_index_granularity_bytes_setting;

DROP TABLE IF EXISTS valid_min_index_granularity_bytes_setting;

SELECT '--- CREATE TABLE with VALID index_granularity_bytes i.e index_granularity_bytes > min_index_granularity_bytes ---';

CREATE TABLE valid_min_index_granularity_bytes_setting
(
  id UInt64,
  value String
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 2024, min_index_granularity_bytes = 1024; -- should NOT result in exception

SELECT '--- INSERT INTO TABLE valid_min_index_granularity_bytes_setting ---';

INSERT INTO valid_min_index_granularity_bytes_setting SELECT number, concat('xxxxxxxxxx', toString(number)) FROM numbers(1000,1000); -- should result in exception

SELECT COUNT(*) from valid_min_index_granularity_bytes_setting WHERE value = 'xxxxxxxxxx1015';

DROP TABLE IF EXISTS valid_min_index_granularity_bytes_setting;
