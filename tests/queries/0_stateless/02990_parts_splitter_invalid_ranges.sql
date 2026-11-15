DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table
(
    `eventType` String,
    `timestamp` UInt64,
    `key` UInt64
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (eventType, timestamp)
ORDER BY (eventType, timestamp, key)
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES test_table;

INSERT INTO test_table VALUES ('1', 1704472004759, 1), ('3', 1704153600000, 2), ('3', 1704153600000, 3), ('5', 1700161822134, 4);

INSERT INTO test_table VALUES ('1', 1704468357009, 1), ('3', 1704153600000, 2), ('3', 1704153600000, 3), ('5', 1701458520878, 4);

INSERT INTO test_table VALUES ('1', 1704470704762, 1), ('3', 1704153600000, 2), ('3', 1704153600000, 3), ('5', 1702609856302, 4);

SELECT eventType, timestamp, key FROM test_table
WHERE (eventType IN ('2', '4')) AND
    ((timestamp >= max2(toInt64('1698938519999'), toUnixTimestamp64Milli(now64() - toIntervalDay(90)))) AND
    (timestamp <= (toInt64('1707143315452') - 1)));

SELECT eventType, timestamp, key FROM test_table FINAL
WHERE (eventType IN ('2', '4')) AND
    ((timestamp >= max2(toInt64('1698938519999'), toUnixTimestamp64Milli(now64() - toIntervalDay(90)))) AND
    (timestamp <= (toInt64('1707143315452') - 1)));

DROP TABLE test_table;
