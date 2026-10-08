SET optimize_read_in_order = 1;
DROP TABLE IF EXISTS mytable;

CREATE TABLE mytable
(
    timestamp        UInt64,
    insert_timestamp UInt64,
    key              UInt64,
    value            Float64
) ENGINE = ReplacingMergeTree(insert_timestamp)
    PRIMARY KEY (key, timestamp)
    ORDER BY (key, timestamp);

INSERT INTO mytable (timestamp, insert_timestamp, key, value) VALUES (1900000010000, 1675159000000, 5, 555), (1900000010000, 1675159770000, 5, -1), (1900000020000, 1675159770000, 5, -0.0002), (1900000030000, 1675159770000, 5, 0), (1900000020000, 1675159700000, 5, 555), (1900000040000, 1675159770000, 5, 0.05), (1900000050000, 1675159770000, 5, 1);

SELECT timestamp, value
FROM mytable FINAL
WHERE key = 5
ORDER BY timestamp DESC;

SELECT if(explain like '%ReadType: InOrder%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT timestamp, value
    FROM mytable FINAL
    WHERE key = 5
    ORDER BY timestamp SETTINGS enable_vertical_final = 0
) WHERE explain like '%ReadType%';


SELECT if(explain like '%ReadType: Default%', 'Ok', 'Error: ' || explain) FROM (
    EXPLAIN PLAN actions = 1
    SELECT timestamp, value
    FROM mytable FINAL
    WHERE key = 5
    ORDER BY timestamp DESC
) WHERE explain like '%ReadType%';


DROP TABLE IF EXISTS mytable;
