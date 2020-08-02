DROP TABLE IF EXISTS pk_01405;
CREATE TABLE pk_01405
(
    `d` Date DEFAULT '2000-01-01',
    `x` DateTime,
    `y` UInt64,
    `z` UInt64
)
ENGINE = MergeTree(d, (toStartOfMinute(x), y, z), 1);

-- no rows required
SELECT
    toUInt32(x),
    y,
    z
FROM pk_01405
WHERE (x >= toDateTime(toDate(toDate(toDate('2018-06-21') % 65537)))) AND (x <= toDateTime(NULL));
