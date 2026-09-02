DROP TABLE IF EXISTS t_update_empty_nested;

CREATE TABLE t_update_empty_nested
(
    `id` UInt32,
    `nested.arr1` Array(UInt64),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

SET mutations_sync = 2;

INSERT INTO t_update_empty_nested SELECT 1, range(number % 10) FROM numbers(100000);
ALTER TABLE t_update_empty_nested ADD COLUMN `nested.arr2` Array(UInt64);

ALTER TABLE t_update_empty_nested UPDATE `nested.arr2` = `nested.arr1` WHERE 1;

SELECT * FROM t_update_empty_nested FORMAT Null;
SELECT sum(length(nested.arr1)), sum(length(nested.arr2)) FROM t_update_empty_nested;

DROP TABLE t_update_empty_nested;
