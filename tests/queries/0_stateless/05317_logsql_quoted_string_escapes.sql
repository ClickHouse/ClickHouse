-- LogsQL quoted strings: escape sequences in "..." and '...', and raw `...` strings without escapes.

DROP TABLE IF EXISTS logs_05317;
CREATE TABLE logs_05317 (`_time` DateTime, `_msg` String) ENGINE = MergeTree ORDER BY _time;
INSERT INTO logs_05317 VALUES
    ('2024-01-01 00:00:01', 'say "hi" now'),
    ('2024-01-01 00:00:02', 'it''s here'),
    ('2024-01-01 00:00:03', 'path C:\\dir\\file'),
    ('2024-01-01 00:00:04', 'col1\tcol2'),
    ('2024-01-01 00:00:05', 'line1\nline2'),
    ('2024-01-01 00:00:06', 'ABC'),
    ('2024-01-01 00:00:07', 'café'),
    ('2024-01-01 00:00:08', 'emoji 😀'),
    ('2024-01-01 00:00:09', 'raw \\t text');

SET enable_logsql_dialect = 1;
SET logsql_table = 'logs_05317';
SET dialect = 'logsql';

"say \"hi\"" | fields _msg;
'it\'s' | fields _msg;
"C:\\dir" | fields _msg;
"col1\tcol2" | fields _msg;
_msg:="line1\nline2" | count();
"\x41\102C" | fields _msg;
"caf\u00e9" | fields _msg;
"\U0001F600" | fields _msg;
`raw \t text` | fields _msg;
`say "hi"` | fields _msg;

"\q" | count(); -- { clientError SYNTAX_ERROR }
"\x4" | count(); -- { clientError SYNTAX_ERROR }
"\400" | count(); -- { clientError SYNTAX_ERROR }
"\uD800" | count(); -- { clientError SYNTAX_ERROR }

SET dialect = 'clickhouse';
DROP TABLE logs_05317;
