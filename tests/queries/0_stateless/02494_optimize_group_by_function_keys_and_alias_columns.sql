CREATE TABLE t(timestamp DateTime, day ALIAS toYYYYMMDD(timestamp)) Engine = MergeTree ORDER BY timestamp;

INSERT INTO t (timestamp) VALUES ('2022-11-25 22:33:19'::DateTime), ('2022-11-25 22:33:19'::DateTime - INTERVAL 1 DAY), ('2022-11-25 22:33:19'::DateTime + INTERVAL 1 DAY), ('2022-11-25 22:33:19'::DateTime - INTERVAL 2 DAY), ('2022-11-25 22:33:19'::DateTime + INTERVAL 2 DAY);
INSERT INTO t (timestamp) VALUES ('2022-11-25 22:33:19'::DateTime), ('2022-11-25 22:33:19'::DateTime - INTERVAL 1 DAY), ('2022-11-25 22:33:19'::DateTime + INTERVAL 1 DAY), ('2022-11-25 22:33:19'::DateTime - INTERVAL 2 DAY), ('2022-11-25 22:33:19'::DateTime + INTERVAL 2 DAY);
INSERT INTO t (timestamp) VALUES ('2022-11-25 22:33:19'::DateTime), ('2022-11-25 22:33:19'::DateTime - INTERVAL 1 DAY), ('2022-11-25 22:33:19'::DateTime + INTERVAL 1 DAY), ('2022-11-25 22:33:19'::DateTime - INTERVAL 2 DAY), ('2022-11-25 22:33:19'::DateTime + INTERVAL 2 DAY);

SELECT day, timestamp FROM remote('127.0.0.{1,2}', currentDatabase(), t) GROUP BY day, timestamp ORDER BY timestamp;
