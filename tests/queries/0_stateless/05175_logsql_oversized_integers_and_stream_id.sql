-- Regressions for the parser-side integer handling of `limit`-style arguments and `len_range`
-- bounds, and for the opaque string semantics of `_stream_id`.

DROP TABLE IF EXISTS logs_05175;
CREATE TABLE logs_05175
(
    `_time` DateTime,
    `_msg` String,
    `_stream_id` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05175 VALUES
    ('2024-01-01 00:00:00', 'a', '00123'),
    ('2024-01-01 00:00:01', 'bb', '123'),
    ('2024-01-01 00:00:02', 'ccc', '18446744073709551616');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_05175';
SET dialect = 'logsql';

-- The largest `UInt64` is parsed exactly instead of rounding through `Float64` to 2^64.
* | limit 18446744073709551615 | count();
* | offset 18446744073709551615 | count();

-- A value above the `UInt64` range is rejected instead of being cast out of range.
* | limit 18446744073709551616 | count(); -- { error SYNTAX_ERROR }
* | limit 99999999999999999999999999 | count(); -- { error SYNTAX_ERROR }
* | offset 18446744073709551616 | count(); -- { error SYNTAX_ERROR }
* | sort by (_time) limit 18446744073709551616 | count(); -- { error SYNTAX_ERROR }
* | top 18446744073709551616 by (_msg); -- { error SYNTAX_ERROR }

-- `len_range()` bounds are parsed on the same exact path.
_msg:len_range(0, 18446744073709551615) | count();
_msg:len_range(18446744073709551615, 18446744073709551615) | count();
_msg:len_range(0, 18446744073709551616) | count(); -- { error SYNTAX_ERROR }
_msg:len_range(18446744073709551616, inf) | count(); -- { error SYNTAX_ERROR }

-- Ordinary bounds keep working.
_msg:len_range(2, 3) | count();
_msg:len_range(2, inf) | count();
_msg:len_range(-inf, 1) | count();

-- A stream id is an opaque identifier: a numeric-looking value stays a string,
-- so a zero-padded id matches itself and not its numeric value.
_stream_id:00123 | fields _stream_id;
_stream_id:123 | fields _stream_id;
_stream_id:in(00123) | fields _stream_id;
_stream_id:in(00123, 123) | fields _stream_id | sort by (_stream_id);

-- An id above the `UInt64` range is not rounded through a numeric literal either.
_stream_id:18446744073709551616 | fields _stream_id;
_stream_id:in(18446744073709551616) | fields _stream_id;

SET dialect = 'clickhouse';
DROP TABLE logs_05175;
