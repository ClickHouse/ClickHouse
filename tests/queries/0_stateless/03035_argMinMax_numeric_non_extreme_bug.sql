CREATE TABLE IF NOT EXISTS test
(
    `value` Float64 CODEC(Delta, LZ4),
    `uuid` LowCardinality(String),
    `time` DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4)
)
ENGINE = MergeTree()
ORDER BY uuid;


INSERT INTO test (uuid, time, value)
VALUES ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:00.000',0), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:09.000',1), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:10.000',2), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:19.000',3), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:20.000',2), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:29.000',1), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:30.000',0),  ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:39.000',-1), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:40.000',-2), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:49.000',-3), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:50.000',-2), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:00:59.000',-1), ('a1000000-0000-0000-0000-0000000000a1','2021-01-01 00:01:00.000',0);

SELECT
    max(time),
    max(toNullable(time)),
    min(time),
    min(toNullable(time)),
    argMax(value, time),
    argMax(value, toNullable(time)),
    argMin(value, time),
    argMin(value, toNullable(time)),
    argMinIf(value, toNullable(time), time != '2021-01-01 00:00:00.000'),
    argMaxIf(value, toNullable(time), time != '2021-01-01 00:00:59.000'),
FROM test
WHERE (time >= fromUnixTimestamp64Milli(1609459200000, 'UTC')) AND (time < fromUnixTimestamp64Milli(1609459260000, 'UTC')) FORMAT Vertical;
