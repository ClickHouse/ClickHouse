DROP TABLE IF EXISTS table_with_single_pk;

CREATE TABLE table_with_single_pk
(
  key UInt8,
  value String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO table_with_single_pk SELECT number, toString(number % 10) FROM numbers(1000000);

-- Check NewPart
SYSTEM FLUSH LOGS;
WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE table = 'table_with_single_pk' AND database = currentDatabase() AND event_type = 'NewPart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
SELECT if(dateDiff('second', toDateTime(time.2), toDateTime(time.1)) = 0, 'ok', 'fail');

-- Now let's check RemovePart
TRUNCATE TABLE table_with_single_pk;
SYSTEM FLUSH LOGS;
WITH (
         SELECT (event_time, event_time_microseconds)
         FROM system.part_log
         WHERE table = 'table_with_single_pk' AND database = currentDatabase() AND event_type = 'RemovePart'
         ORDER BY event_time DESC
         LIMIT 1
    ) AS time
SELECT if(dateDiff('second', toDateTime(time.2), toDateTime(time.1)) = 0, 'ok', 'fail');

DROP TABLE table_with_single_pk;
