SET session_timezone = 'UTC';

CREATE TABLE logs_04869
(
    `_time` DateTime,
    `_msg` String,
    `payload` String,
    `level` String,
    `size` UInt64,
    `nullable` Nullable(String)
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04869 VALUES
    ('2024-01-01 00:00:00', 'id=5', '{"size":"7"}', 'error', 5, NULL),
    ('2024-01-01 00:01:00', 'id=30', '{"size":"40"}', 'info', 30, '');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04869';
SET dialect = 'logsql';

-- Text filters stringify typed fields, and a missing Nullable field is empty.
size:* | count();
size:i(""*) | count();
nullable:"" | count();

-- Numeric buckets parse String fields row-by-row instead of doing arithmetic on strings.
* | stats by (size:10) count() | sort by (size);

-- Text pipes replace typed target fields with their LogsQL string values.
* | format if (level:error) "X" as size | fields size | sort by (_time);
* | format "X" as size keep_original_fields | fields size | sort by (_time);
* | extract if (level:error) "id=<size>" | fields size | sort by (_time);
* | unpack_json from payload fields (size) | fields size | sort by (_time);

DROP TABLE logs_04869;
