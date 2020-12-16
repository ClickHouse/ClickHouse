DROP TABLE IF EXISTS t;
CREATE TABLE t (x String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES ('Hello');

SET max_parallel_replicas = 3;
SELECT * FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

DROP TABLE t;

CREATE TABLE t (x String) ENGINE = MergeTree ORDER BY cityHash64(x) SAMPLE BY cityHash64(x);
INSERT INTO t SELECT toString(number) FROM numbers(1000);

SET max_parallel_replicas = 1;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

SET max_parallel_replicas = 2;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

SET max_parallel_replicas = 3;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t);

DROP TABLE t;
