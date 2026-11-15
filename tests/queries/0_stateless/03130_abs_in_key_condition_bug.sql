DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, ts DateTime) ENGINE = MergeTree() ORDER BY (id, ts) SETTINGS index_granularity = 2;

INSERT INTO t VALUES
    (1, toDateTime('2023-05-04 21:17:23', 'UTC')), (1, toDateTime('2023-05-04 22:17:23', 'UTC')), (2, toDateTime('2023-05-04 22:17:23', 'UTC')), (2, toDateTime('2023-05-04 23:17:23', 'UTC'));

SELECT  count(abs(toUnixTimestamp(ts, 'UTC') - toUnixTimestamp('2023-05-04 22:17:23', 'UTC')) AS error) FROM t WHERE error < 3600;

DROP TABLE IF EXISTS t;
