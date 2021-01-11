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

INSERT INTO ttl_where SELECT toDate('2000-10-10'), number FROM numbers(10);
INSERT INTO ttl_where SELECT toDate('1970-10-10'), number FROM numbers(10);

OPTIMIZE TABLE ttl_where FINAL;

SELECT * FROM ttl_where ORDER BY d, i;

DROP TABLE ttl_where;
