-- Decimal-backed constants (Decimal*, DateTime64) must be serialized to remote shards exactly:
-- a bare numeric literal would be re-parsed as Float64 (precision loss) and a quoted string cast
-- straight to DateTime64 fails to parse for values such as "0".
-- https://github.com/ClickHouse/ClickHouse/issues/94612

DROP TABLE IF EXISTS ts_data_94612;
CREATE TABLE ts_data_94612
(
    device_id UInt32,
    data_item_id UInt32,
    data_time DateTime64(3, 'UTC'),
    data_value UInt64
)
ENGINE = MergeTree ORDER BY (device_id, data_item_id, data_time);

INSERT INTO ts_data_94612 VALUES (100, 1, fromUnixTimestamp64Milli(1697547086760), 3), (100, 1, fromUnixTimestamp64Milli(1697547086761), 4);

-- Used to fail on the shard with: Cannot parse DateTime (CANNOT_PARSE_DATETIME).
SELECT data_time, max(data_value)
FROM
(
    SELECT device_id, data_item_id, data_time, max(data_value) AS data_value
    FROM remote('127.0.0.{1,2}', currentDatabase(), ts_data_94612)
    GROUP BY device_id, data_item_id, data_time
)
WHERE data_time >= fromUnixTimestamp64Milli(0, 'UTC')
GROUP BY data_time
ORDER BY data_time;

-- A high-scale Decimal constant must reach every shard exactly. Without an exact serialization
-- the remote shard re-parses the literal as Float64 and rounds the last digit, so DISTINCT would
-- return two different values instead of one.
SELECT DISTINCT materialize(toDecimal64('123456789012.34567', 5)) AS c
FROM remote('127.0.0.{1,2}', system.one);

-- A nanosecond DateTime64 constant (19 significant digits, overflows Decimal64) must also be exact.
SELECT DISTINCT materialize(toDateTime64('2023-10-17 12:51:26.123456789', 9, 'UTC')) AS ts
FROM remote('127.0.0.{1,2}', system.one);

-- Decimals nested in Array/Tuple/Map must be exact too. Without an exact serialization the remote
-- shard rounds the value and the differing column name makes the query fail with NOT_FOUND_COLUMN_IN_BLOCK.
SELECT DISTINCT materialize([toDecimal64('123456789012.34567', 5)]) AS c
FROM remote('127.0.0.{1,2}', system.one);

SELECT DISTINCT materialize((toDecimal64('123456789012.34567', 5), 'x')) AS c
FROM remote('127.0.0.{1,2}', system.one);

SELECT DISTINCT materialize(map('k', toDecimal64('123456789012.34567', 5))) AS c
FROM remote('127.0.0.{1,2}', system.one);

DROP TABLE ts_data_94612;
