CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree() ORDER BY (id, ts) SETTINGS index_granularity = 2;

INSERT INTO t VALUES (1, '2023-05-04 21:17:23') (1, '2023-05-04 22:17:23') (2, '2023-05-04 22:17:23') (2, '2023-05-04 23:17:23');

SELECT *, abs(toUnixTimestamp(ts) - toUnixTimestamp(1683238643)) AS error FROM t WHERE error < 3600;

