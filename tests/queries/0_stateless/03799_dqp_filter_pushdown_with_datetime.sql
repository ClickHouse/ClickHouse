-- Tags: no-random-merge-tree-settings
DROP TABLE IF EXISTS ts_data_double_raw;
CREATE TABLE  ts_data_double_raw
(
   device_id UInt32,
   data_item_id UInt32,
   data_time DateTime64(3, 'UTC'),
   data_value UInt64
)
ENGINE = MergeTree ORDER BY (device_id, data_item_id, data_time) SETTINGS index_granularity = 8192;

INSERT INTO ts_data_double_raw VALUES (100, 1, fromUnixTimestamp64Milli(1697547086760), 3), (100, 1, fromUnixTimestamp64Milli(1697547086761), 4);
INSERT INTO ts_data_double_raw VALUES (100, 1, fromUnixTimestamp64Milli(1697547086762), 5), (100, 1, fromUnixTimestamp64Milli(1697547086763), 6);

-- predicate push down
SELECT *
FROM
(
    SELECT
        device_id,
        data_item_id,
        data_time,
        max(data_value)
    FROM remote('127.0.0.{1,2}', currentDatabase(), ts_data_double_raw)
    GROUP BY
        device_id,
        data_item_id,
        data_time
)
WHERE data_time >= fromUnixTimestamp64Milli(0, 'UTC')
format Null;

-- HAVING
SELECT
    data_time,
    fromUnixTimestamp64Milli(0, 'UTC')
FROM remote('127.0.0.{1,2}', currentDatabase(), ts_data_double_raw)
GROUP BY data_time
HAVING data_time > fromUnixTimestamp64Milli(0, 'UTC')
format Null;
