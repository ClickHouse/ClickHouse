DROP TABLE IF EXISTS table_with_single_pk;

CREATE TABLE table_with_single_pk
(
  key UInt8,
  value String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO table_with_single_pk SELECT number, toString(number % 10) FROM numbers(10000000);

SYSTEM FLUSH LOGS;

WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
SELECT if(dateDiff('second', toDateTime(time.2), toDateTime(time.1)) = 0, 'ok', 'fail');

DROP TABLE IF EXISTS table_with_single_pk;
