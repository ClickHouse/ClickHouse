DROP TABLE IF EXISTS invalid_min_index_granularity_bytes_setting;

CREATE TABLE invalid_min_index_granularity_bytes_setting
(
  id UInt64,
  value String
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 1, min_index_granularity_bytes = 1024; -- { serverError 36 }

DROP TABLE IF EXISTS valid_min_index_granularity_bytes_setting;

CREATE TABLE valid_min_index_granularity_bytes_setting
(
  id UInt64,
  value String
) ENGINE MergeTree()
ORDER BY id SETTINGS index_granularity_bytes = 2024, min_index_granularity_bytes = 1024;

INSERT INTO valid_min_index_granularity_bytes_setting SELECT number, concat('xxxxxxxxxx', toString(number)) FROM numbers(1000,1000);

SELECT COUNT(*) from valid_min_index_granularity_bytes_setting WHERE value = 'xxxxxxxxxx1015';

DROP TABLE IF EXISTS valid_min_index_granularity_bytes_setting;
