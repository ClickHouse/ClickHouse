-- Tags: replica

DROP TABLE IF EXISTS t;

CREATE TABLE t (x String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES ('Hello');

SET max_parallel_replicas = 3;
SET parallel_replicas_mode = 'custom_key';
SET parallel_replicas_custom_key_filter_type = 'range';

SELECT * FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'sipHash64(x)';

DROP TABLE t;

CREATE TABLE t (x String, y UInt32) ENGINE = MergeTree ORDER BY cityHash64(x) SAMPLE BY cityHash64(x);
INSERT INTO t SELECT toString(number), number FROM numbers(1000);

SET max_parallel_replicas = 1;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'y';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x) + y';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x)';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x) + 1';

SET max_parallel_replicas = 2;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'y';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x) + y';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x)';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x) + 1';

SET max_parallel_replicas = 3;
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'y';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x) + y';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x)';
SELECT count() FROM remote('127.0.0.{2|3|4}', currentDatabase(), t) SETTINGS parallel_replicas_custom_key = 'cityHash64(x) + 1';

DROP TABLE t;
