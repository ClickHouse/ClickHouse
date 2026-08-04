SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04615;
CREATE TABLE logs_04615
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `app` String,
    `code` UInt16,
    `size` UInt64,
    `ip` String,
    `user_id` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04615 VALUES
    ('2024-01-01 10:00:00', 'error: cannot open file /var/lib/data', 'error', 'nginx', 500, 1024, '127.0.0.1', 'u1'),
    ('2024-01-01 10:01:00', 'connection accepted from 10.2.3.4', 'info', 'nginx', 200, 100, '10.2.3.4', 'u2'),
    ('2024-01-01 10:02:00', 'Error while processing request', 'error', 'app-server', 500, 2048, '10.2.3.5', 'u1'),
    ('2024-01-01 10:03:00', 'request finished in 25ms', 'info', 'app-server', 200, 512, '10.2.3.4', 'u3'),
    ('2024-01-01 10:04:00', 'warning: disk space low', 'warn', 'monitor', 0, 42, '192.168.1.10', 'u2'),
    ('2024-01-02 12:00:00', 'ssh: login fail for user "root"', 'error', 'sshd', 401, 77, '192.168.1.11', 'u4'),
    ('2024-01-02 12:01:00', 'user logged in successfully', 'info', 'sshd', 200, 88, '192.168.1.11', 'u4'),
    ('2024-01-02 12:02:00', 'fatal error: cannot allocate memory', 'fatal', 'app-server', 500, 4096, '10.2.3.6', 'u5');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04615';
SET dialect = 'logsql';

-- Word filter (matches with token boundaries, case-sensitively).
error | fields _msg | sort by (_msg);

-- Case-insensitive filter.
i(error) | fields _msg | sort by (_msg);

-- Phrase filter.
"cannot open file" | fields _msg;

-- Prefix filter.
warn* | fields _msg | sort by (_msg);

-- Substring filter.
*ccept* | fields _msg;

-- Field filter and exact filter.
level:error | fields _msg | sort by (_msg);
level:=fatal | fields _msg;

-- Exact prefix.
app:="app"* | fields _msg | sort by (_msg);

-- Negation and logical operators.
level:error -sshd | fields _msg | sort by (_msg);
level:(error or fatal) app:app-server | fields _msg | sort by (_msg);
NOT level:info NOT level:warn | fields _msg | sort by (_msg);

-- Regexp filter.
~"cannot (open|allocate)" | fields _msg | sort by (_msg);
level:~"^(warn|fatal)$" | fields _msg | sort by (_msg);

-- Range comparisons over a numeric field.
size:>1000 | fields _msg | sort by (_msg);
code:>=401 code:<=500 | fields _msg | sort by (_msg);
size:range[42, 100] | fields _msg | sort by (_msg);

-- in() filter.
level:in(warn, fatal) | fields _msg | sort by (_msg);

-- contains_any / contains_all.
_msg:contains_any(ssh, disk) | fields _msg | sort by (_msg);
_msg:contains_all(error, memory) | fields _msg;

-- seq() filter.
seq("error", "file") | fields _msg;

-- len_range and string_range.
_msg:len_range(1, 30) | fields _msg | sort by (_msg);
level:string_range(e, g) | fields _msg | sort by (_msg);

-- ipv4_range.
ip:ipv4_range("192.168.0.0/16") | fields _msg | sort by (_msg);

-- eq_field.
user_id:eq_field(user_id) | count();

-- Stream filter syntax.
{app="sshd", level="error"} | fields _msg;
{app in ("nginx", "monitor")} | count();

-- Time filters.
_time:[2024-01-01Z, 2024-01-02Z) | count();
_time:2024-01-02Z | count();
_time:>=2024-01-02T12:01:00Z | fields _msg | sort by (_msg);
_time:day_range[10:00, 10:02] | fields _msg | sort by (_msg);

-- Empty and non-empty values.
level:"" | count();
level:* | count();

-- Match-all with pipes.
* | count();
* | limit 3 | count();
* | sort by (_time) | offset 6 | fields _msg;

-- fields, delete, copy, rename.
level:=warn | fields _time, _msg, level;
level:=warn | delete _time, _msg, code, size, ip, user_id;
level:=warn | fields level | copy level as level_copy;
level:=warn | fields level | rename level as severity;

-- sort with desc and limit.
* | sort by (size) desc limit 3 | fields size;
* | sort by (level, _time desc) limit 4 | fields level, size;

-- first and last.
* | first 2 by (size) | fields size;
* | last 2 by (size) | fields size;

-- stats.
* | stats count();
* | count();
* | stats by (level) count() as total, sum(size) as bytes | sort by (level);
* | stats by (app) count_uniq(user_id) as users | sort by (app);
* | stats count() if (level:error) as errors, count() as total;
* | stats by (_time:1d) count() as per_day | sort by (_time);
* | stats max(size), min(size), avg(code);
* | stats by (level) values(user_id) limit 1 as sample_user | sort by (level) | fields level;
* | stats quantile(0.5, size) as median_size;
level:error | stats by (app) count() | sort by (app);

-- uniq and top.
* | uniq by (level) | sort by (level);
* | uniq by (level) with hits | sort by (level);
* | top 2 by (level);

-- where after stats and math.
* | stats by (app) count() as c | where c:>1 | sort by (app);
* | stats by (app) count() as c, count() if (level:error) as errors | math (errors / c) as error_rate | where error_rate:>=0.5 | sort by (app) | fields app, error_rate;

-- in(subquery).
user_id:in(level:error | fields user_id) level:info | fields _msg | sort by (_msg);

-- Comments and options.
error # find errors
    | fields _msg | sort by (_msg);
options(concurrency=2) level:=fatal | count();

SET dialect = 'clickhouse';
DROP TABLE logs_04615;

-- A multi-line query with a LogsQL comment.
SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04615_multiline';
CREATE TABLE logs_04615_multiline (`_time` DateTime, `_msg` String, level String) ENGINE = MergeTree ORDER BY _time;
INSERT INTO logs_04615_multiline VALUES ('2024-01-01 00:00:00', 'error one', 'error'), ('2024-01-01 00:00:01', 'fine', 'info');
SET dialect = 'logsql';
level:error       # count errors
    | stats by (level)
        count() as c
    | sort by (level);
SET dialect = 'clickhouse';
DROP TABLE logs_04615_multiline;
