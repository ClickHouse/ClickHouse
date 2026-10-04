DROP TABLE IF EXISTS logs_04908;
CREATE TABLE logs_04908
(
    `_time` DateTime,
    `_msg` String,
    `decimal_value` Decimal128(2),
    `size` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04908 VALUES
    ('2024-01-01 00:00:00', 'negative fraction', -0.25, '1KB'),
    ('2024-01-01 00:00:01', 'positive fraction', 15.5, '2KB'),
    ('2024-01-01 00:00:02', 'integer', 10, '3KB');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04908';
SET dialect = 'logsql';

-- An integral bucket step keeps exact integer values, but typed Decimal values
-- with a fractional part must use the decimal path rather than being truncated.
* | stats by (decimal_value:10) count() as c | sort by (decimal_value);

-- Compound duration and byte literals have one optional leading sign only.
_time:>1h-30m | stats count(); -- { error SYNTAX_ERROR }
size:>1KB-512 | stats count(); -- { error SYNTAX_ERROR }

SET dialect = 'clickhouse';
DROP TABLE logs_04908;
