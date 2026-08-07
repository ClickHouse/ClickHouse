SELECT toStartOfWeek(toDateTime('1970-01-01 00:00:00', 'UTC'));
SELECT toStartOfWeek(toDateTime('1970-01-01 00:00:00', 'Asia/Istanbul'));
SELECT toStartOfWeek(toDateTime('1970-01-01 00:00:00', 'Canada/Atlantic'));
SELECT toStartOfWeek(toDateTime('1970-01-04 00:00:00'));
 

DROP TABLE IF EXISTS t02176;
CREATE TABLE t02176(timestamp DateTime) ENGINE = MergeTree
PARTITION BY toStartOfWeek(timestamp)
ORDER BY tuple();

INSERT INTO t02176 VALUES (1559952000);

SELECT count() FROM t02176 WHERE timestamp >= toDateTime('1970-01-01 00:00:00');
SELECT count() FROM t02176 WHERE identity(timestamp) >= toDateTime('1970-01-01 00:00:00');

DROP TABLE t02176;
