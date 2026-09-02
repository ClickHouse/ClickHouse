DROP TABLE IF EXISTS ttl;

CREATE TABLE ttl
(
    `a` UInt32,
    `timestamp` DateTime
)
ENGINE = MergeTree
ORDER BY a
TTL timestamp + toIntervalSecond(2) WHERE a IN (
    SELECT number
    FROM system.numbers
    LIMIT 100000
);

SHOW CREATE ttl;
DROP TABLE ttl;
