DROP TABLE IF EXISTS derived_metrics_local;

CREATE TABLE derived_metrics_local
(
  timestamp DateTime,
  bytes UInt64
)
ENGINE=SummingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (toStartOfHour(timestamp), timestamp)
TTL toStartOfHour(timestamp) + INTERVAL 1 HOUR GROUP BY toStartOfHour(timestamp)
SET bytes=max(bytes);

INSERT INTO derived_metrics_local values('2020-01-01 00:00:00', 1);
INSERT INTO derived_metrics_local values('2020-01-01 00:01:00', 3);
INSERT INTO derived_metrics_local values('2020-01-01 00:02:00', 2);

OPTIMIZE TABLE derived_metrics_local FINAL;
SELECT * FROM derived_metrics_local;

DROP TABLE derived_metrics_local;

CREATE TABLE derived_metrics_local 
(
  timestamp DateTime,
  timestamp_h DateTime materialized toStartOfHour(timestamp),
  bytes UInt64
)
ENGINE=SummingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp_h, timestamp)
TTL toStartOfHour(timestamp) + INTERVAL 1 HOUR GROUP BY timestamp_h
SET bytes=max(bytes), timestamp = toStartOfHour(any(timestamp));

INSERT INTO derived_metrics_local values('2020-01-01 00:01:00', 111);
INSERT INTO derived_metrics_local values('2020-01-01 00:19:22', 22);
INSERT INTO derived_metrics_local values('2020-01-01 00:59:02', 1);

OPTIMIZE TABLE derived_metrics_local FINAL;
SELECT timestamp, timestamp_h, bytes FROM derived_metrics_local;

DROP TABLE IF EXISTS derived_metrics_local;

CREATE TABLE derived_metrics_local
(
  timestamp DateTime,
  bytes UInt64 TTL toStartOfHour(timestamp) + INTERVAL 1 HOUR
)
ENGINE=MergeTree()
ORDER BY (toStartOfHour(timestamp), timestamp)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO derived_metrics_local values('2020-01-01 00:01:00', 111) ('2020-01-01 00:19:22', 22) ('2100-01-01 00:19:22', 1);

OPTIMIZE TABLE derived_metrics_local FINAL;
SELECT sum(bytes) FROM derived_metrics_local;

DROP TABLE IF EXISTS derived_metrics_local;

CREATE TABLE derived_metrics_local
(
  timestamp DateTime,
  bytes UInt64
)
ENGINE=MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (toStartOfHour(timestamp), timestamp)
TTL toStartOfHour(timestamp) + INTERVAL 1 HOUR
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO derived_metrics_local values('2020-01-01 00:01:00', 111);
INSERT INTO derived_metrics_local values('2020-01-01 00:19:22', 22);
INSERT INTO derived_metrics_local values('2020-01-01 00:59:02', 1);

OPTIMIZE TABLE derived_metrics_local FINAL;
SELECT count() FROM derived_metrics_local;

DROP TABLE IF EXISTS derived_metrics_local;
