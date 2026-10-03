SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04891;
CREATE TABLE logs_04891
(
    `_time` DateTime,
    `_msg` String,
    `payload` String,
    `level` String,
    `size` UInt64,
    `bucket` String,
    `fractional_bucket` String,
    `nullable` Nullable(String)
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04891 VALUES
    ('2024-01-01 00:00:00', 'id=5', '{"size":"7"}', 'error', 5, '5', '15.5', NULL),
    ('2024-01-01 00:01:00', 'id=30', '{"size":"40"}', 'info', 30, '30', '-0.25', '');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04891';
SET dialect = 'logsql';

-- Selected text filters stringify typed fields; an empty Nullable field matches an empty filter.
size:* | count();
size:i(""*) | count();
size:i(*) | count();
nullable:"" | count();

-- An inclusive range whose endpoints are the same instant matches that instant.
_time:[2024-01-01T00:00:00Z, 2024-01-01T00:00:00Z] | count();

-- Numeric buckets parse String fields row-by-row instead of doing arithmetic on strings.
* | stats by (bucket:10) count() | sort by (bucket);
* | stats by (bucket:1) count() | sort by (bucket);
* | stats by (fractional_bucket:10) count() | sort by (fractional_bucket);
* | stats by (size:1) count() | rename size as renamed_size | sort by (renamed_size);
* | stats by (size:1) count() | copy size as copied_size | sort by (copied_size);

-- Ranking and partitioning operate on the schema produced by a projection pipe.
* | fields level | sort by (level) partition by (level) limit 1;
* | fields level | sort by (level) rank as position | fields position, level;

-- Text pipes replace typed target fields with their LogsQL string values.
* | format if (level:error) "X" as size | fields size | sort by (_time);
* | format "X" as size keep_original_fields | fields size | sort by (_time);
* | extract if (level:error) "id=<size>" | fields size | sort by (_time);
* | unpack_json from payload fields (size) | fields size | sort by (_time);

SET dialect = 'clickhouse';
DROP TABLE logs_04891;
