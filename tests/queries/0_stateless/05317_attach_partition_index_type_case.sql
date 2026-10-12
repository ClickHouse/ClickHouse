-- The type of a skip index is case-insensitive, so `TYPE SET(0)` and `TYPE set(0)`
-- declare the same index and the partition can be attached.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (a UInt64, b UInt64, INDEX idx b TYPE SET(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2;
CREATE TABLE dst (a UInt64, b UInt64, INDEX idx b TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2;

INSERT INTO src VALUES (1, 10), (3, 30), (2, 20);

ALTER TABLE dst ATTACH PARTITION 1 FROM src;
SELECT a, b FROM dst ORDER BY a;

-- A different index type is still rejected.
DROP TABLE dst;
CREATE TABLE dst (a UInt64, b UInt64, INDEX idx b TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2;
ALTER TABLE dst ATTACH PARTITION 1 FROM src; -- { serverError BAD_ARGUMENTS }

DROP TABLE src;
DROP TABLE dst;
