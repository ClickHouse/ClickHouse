DROP TABLE IF EXISTS random_mt;

CREATE TABLE random_mt
(
    key UInt64,
    value String
)
ENGINE MergeTree()
ORDER BY tuple();

INSERT INTO random_mt VALUES (1, 'Hello');

SELECT any(value > 0) from system.events WHERE event = 'PartsLockHoldMicroseconds' or event = 'PartsLockWaitMicroseconds';

DROP TABLE IF EXISTS random_mt;

