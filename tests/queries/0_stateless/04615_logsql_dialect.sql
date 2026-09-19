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

-- New features: text pipes, join, union, window pipes, extended stats.
SET allow_experimental_logsql_dialect = 1;
SET session_timezone = 'UTC';
DROP TABLE IF EXISTS logs_04615_b;
CREATE TABLE logs_04615_b
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `app` String,
    `size` UInt64,
    `payload` String,
    `tags` String,
    `_stream_id` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04615_b VALUES
    ('2024-01-01 10:00:00', 'ip=1.2.3.4 user=alice dur=15ms', 'info', 'web', 100, '{"a":"x","n":42,"nested":{"b":"y"}}', '["prod","eu"]', 's1'),
    ('2024-01-01 10:01:00', 'ip=5.6.7.8 user=bob dur=25ms', 'warn', 'web', 200, '{"a":"z","n":7}', '["dev"]', 's2'),
    ('2024-01-01 10:02:00', 'no ip here', 'error', 'api', 300, 'not json', '[]', 's1');

SET logsql_table = 'logs_04615_b';
SET dialect = 'logsql';

-- extract
* | extract "ip=<ip> user=<user> " | fields _time, ip, user | sort by (_time);

-- extract_regexp
* | extract_regexp "user=(?P<username>[a-z]+)" | fields username | sort by (username);

-- format
level:=warn | format "app=<app> level=<level> size=<size>" as summary | fields summary;

-- unpack_json
* | unpack_json from payload fields (a, n, nested.b) | fields a, n, nested.b | sort by (a);

-- json_array filters and pipes
tags:json_array_contains_any("prod", "staging") | fields _msg;
* | json_array_len(tags) as n_tags | fields n_tags | sort by (n_tags);
* | json_array_concat "," from tags as tag_list | fields tag_list | sort by (tag_list);

-- unroll
* | unroll (tags) | fields tags | sort by (tags);

-- split and unpack_words
level:=error | split " " as words | fields words;
level:=error | unpack_words as words drop_duplicates | fields words;

-- len, hash
level:=warn | len(app) as app_len | fields app_len;

-- coalesce
* | coalesce(level, app) as lvl | fields lvl | sort by (lvl);

-- replace and replace_regexp
level:=error | replace ("here", "there") | fields _msg;
level:=warn | replace_regexp ("[0-9]+", "N") at _msg | fields _msg;

-- pack_json / pack_logfmt
level:=error | pack_json fields (level, app) as packed | fields packed;
level:=error | pack_logfmt fields (level, app) as packed | fields packed;

-- _stream_id filter
_stream_id:s1 | count();
_stream_id:in(s1, s2) | count();

-- ipv6_range
* | count() ; # separator
"::1" ipv6_range("::1") | count();

-- common case filters
i(IP) | count();
_msg:contains_common_case("Ip") | count();
level:equals_common_case("Error") | count();

-- pattern_match
pattern_match("ip=<IP4>") | count();
_msg:pattern_match_prefix("ip=<IP4> user=<W>") | fields _msg | sort by (_msg);

-- multi-field and row stats
* | stats sum(size, size) as double_size, min(size, size) as min_size;
* | stats field_max(size, app) as biggest_app;
* | stats row_max(size, level, app) as top_row;
* | stats json_values(level) limit 5 as all_levels;
* | stats count(level, app) as with_any;

-- switch
* | stats count() switch(case (level:=info) as infos, case (app:=web) as webs, default as others);

-- rate with a time range
_time:[2024-01-01Z, 2024-01-02Z) | stats rate() as per_second;

-- stats by bucket offset and subnet
* | stats by (size:100 offset 50) count() as c | sort by (size);

-- sort with rank and partition
* | sort by (size) rank as position | fields position, size;
* | sort by (size desc) partition by (app) limit 1 | fields app, size | sort by (app);
* | first 1 by (size) rank | fields rank, size;

-- running_stats and total_stats
* | running_stats sum(size) as running_size | fields _time, running_size | sort by (_time);
* | total_stats by (app) count() as app_total | fields _time, app, app_total | sort by (_time);

-- union and join
level:=error | fields _msg | union (level:=warn | fields _msg) | sort by (_msg);
* | join by (app) (app:=web | stats by (app) count() as app_hits) | fields _time, app, app_hits | sort by (_time);

-- options
options(time_offset=0s) level:=error | count();
options(global_filter=(level:=error)) * | count();

SET dialect = 'clickhouse';
DROP TABLE logs_04615_b;
