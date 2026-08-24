SELECT 'TTL WHERE';
DROP TABLE IF EXISTS ttl_where;

CREATE TABLE ttl_where
(
    `d` Date,
    `i` UInt32
)
ENGINE = MergeTree
ORDER BY tuple()
TTL d + toIntervalYear(10) DELETE WHERE i % 3 = 0,
    d + toIntervalYear(40) DELETE WHERE i % 3 = 1;

-- This test will fail at 2040-10-10

INSERT INTO ttl_where SELECT toDate('2000-10-10'), number FROM numbers(10);
INSERT INTO ttl_where SELECT toDate('1970-10-10'), number FROM numbers(10);
OPTIMIZE TABLE ttl_where FINAL;

SELECT * FROM ttl_where ORDER BY d, i;

DROP TABLE ttl_where;

SELECT 'TTL GROUP BY';
DROP TABLE IF EXISTS ttl_group_by;

CREATE TABLE ttl_group_by
(
    `d` Date,
    `i` UInt32,
    `v` UInt64
)
ENGINE = MergeTree
ORDER BY (toStartOfMonth(d), i % 10)
TTL d + toIntervalYear(10) GROUP BY toStartOfMonth(d), i % 10 SET d = any(toStartOfMonth(d)), i = any(i % 10), v = sum(v),
    d + toIntervalYear(40) GROUP BY toStartOfMonth(d) SET d = any(toStartOfMonth(d)), v = sum(v);

INSERT INTO ttl_group_by SELECT toDate('2000-10-10'), number, number FROM numbers(100);
INSERT INTO ttl_group_by SELECT toDate('1970-10-10'), number, number FROM numbers(100);
OPTIMIZE TABLE ttl_group_by FINAL;

SELECT * FROM ttl_group_by ORDER BY d, i;

DROP TABLE ttl_group_by;
