#!/usr/bin/env bash
# Tags: long
# It is a corpus of hundreds of queries, and it can take more than three minutes under sanitizers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This test runs the queries from the parser tests of VictoriaLogs (lib/logstorage/parser_test.go
# in the VictoriaMetrics/VictoriaLogs repository, Apache 2.0) through the logsql dialect.

# The dialect is gated by a setting, and SET queries still work when it is off, so that users can recover.
$CLICKHOUSE_CLIENT --dialect logsql -q "error" |& grep -om1 "SUPPORT_IS_DISABLED"
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --dialect logsql -q "error" |& grep -om1 "INVALID_SETTING_VALUE"
$CLICKHOUSE_CLIENT --dialect logsql -q "SET dialect = 'clickhouse'" && echo "SET works with the dialect disabled"

# The _time and _msg fields can be mapped to arbitrary columns.
$CLICKHOUSE_CLIENT -q "CREATE TABLE text_log_style_04616 (event_time DateTime, message String) ENGINE = MergeTree ORDER BY event_time"
$CLICKHOUSE_CLIENT -q "INSERT INTO text_log_style_04616 VALUES ('2024-01-01 00:00:00', 'an error happened'), ('2024-01-01 00:00:01', 'all good')"
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table text_log_style_04616 \
    --logsql_time_column event_time --logsql_message_column message --dialect logsql \
    -q "error _time:>=2024-01-01T00:00:00 | count()"
$CLICKHOUSE_CLIENT -q "DROP TABLE text_log_style_04616"

# Unsupported LogsQL features are reported clearly.
$CLICKHOUSE_CLIENT --allow_experimental_logsql_dialect 1 --logsql_table corpus_logs_04616 --dialect logsql -q "* | unpack_json" |& grep -om1 "NOT_IMPLEMENTED"

$CLICKHOUSE_CLIENT -q 'CREATE TABLE corpus_logs_04616
(
    `_time` DateTime64(9),
    `_msg` String,
    `foo` String,
    `bar` String,
    `baz` String,
    `a` String,
    `b` String,
    `c` String,
    `x` String,
    `y` String,
    `z` String,
    `level` String,
    `user` String,
    `ip` String,
    `host` String,
    `app` String,
    `aa` String,
    `bb` String,
    `abc` String,
    `response_size` String,
    `request_duration` String,
    `client_ip` String,
    `server_ip` String,
    `is_admin` String,
    `rows` String,
    `dict` String,
    `f1` String,
    `f2` String,
    `f3` String,
    `some_field` String,
    `Time` String,
    `id.foo.bar` String,
    `total` String,
    `duration` String,
    `size` String,
    `field` String
) ENGINE = MergeTree ORDER BY _time'

LOGSQL_OPTS=(--allow_experimental_logsql_dialect 1 --logsql_table corpus_logs_04616 --dialect logsql)

# All these queries from the VictoriaLogs test suite must parse and execute successfully.
VALID_QUERIES=$CLICKHOUSE_TMP/logsql_valid_queries.sql
cat > "$VALID_QUERIES" <<'CORPUS_EOF'
foo
;
"":foo
;
foo  :  bar
;
foo::bar
;
foo :  :bar
;
foo:(:bar)
;
foo : ( :bar )
;
foo: ::: bar
;
1 - 2
;
1 ~2
;
1* 2
;
1 * 2
;
"" bar
;
!''
;
-''
;
foo:""
;
-foo:""
;
!foo:""
;
not foo:""
;
not(foo)
;
not (foo)
;
not ( foo or bar )
;
!(foo or bar)
;
-(foo or bar)
;
foo:!""
;
_msg:foo
;
'foo:bar'
;
'!foo'
;
'-foo'
;
'{a="b"}'
;
foo 'and' and bar
;
foo bar
;
foo and bar
;
foo AND bar
;
foo or bar
;
foo OR bar
;
not foo
;
! foo
;
- foo
;
not !`foo bar`
;
not -`foo bar`
;
foo or bar and not baz
;
'foo bar' !baz
;
foo:!bar
;
foo:-bar
;
foo and bar and baz or x or y or z and zz
;
foo and bar and (baz or x or y or z) and zz
;
(foo or bar or baz) and x and y and (z or zz)
;
(foo or bar or baz) and x and y and not (z or zz)
;
NOT foo AND bar OR baz
;
NOT (foo AND bar) OR baz
;
foo OR bar AND baz
;
foo bar or baz xyz
;
foo (bar or baz) xyz
;
foo or bar baz or xyz
;
(foo or bar) (baz or xyz)
;
(foo OR bar) AND baz
;
'stats' foo
;
'stats_remote' abc
;
"filter" bar copy fields avg baz
;
foo:(bar baz or not :xxx)
;
(foo:bar and (foo:baz or aa:bb) and xx) and y
;
level:error and _msg:(a or b)
;
level: ( ((error or warn*) and re(foo))) (not (bar))
;
!(foo bar or baz and not aa*)
;
(foo AND bar) AND (baz AND x:y)
;
(foo AND bar) OR (baz AND x:y)
;
(foo OR bar) OR (baz OR x:y)
;
(foo OR bar) AND (baz OR x:y)
;
'foo'* and (a:x* and x:* or y:i(""*)) and i("abc def"*)
;
foo *
;
"foo" *
;
*foo*
;
foo:*bar*
;
foo: *"bar*:baz"*
;
"" or foo:"" and not bar:""
;
_stream_id:in()
;
_stream_id:in(*)
;
'_stream_id':in(*)
;
_stream:{}
;
_stream : { foo =  bar , }  
;
"_stream":{}
;
{}
;
_time:[-5m,now)
;
_time:(  now-1h  , now-5m34s5ms]
;
_time:[2023, 2023-01)
;
_time:[2023-01-02, 2023-02-03T04)
;
_time:[2023-01-02T04:05, 2023-02-03T04:05:06)
;
_time:[2023-01-02T04:05:06Z, 2023-02-03T04:05:06.234Z)
;
_time:[2023-01-02T04:05:06+02:30, 2023-02-03T04:05:06.234-02:45)
;
_time:[2023-06-07T23:56:34.3456-02:30, now)
;
_time:("2024-01-02+02:00", now)
;
_time:now
;
_time:>now
;
_time:>=now
;
_time:<=now
;
_time:<now
;
_time:2024
;
_time:2024Z
;
_time:"2024Z"
;
_time:=2024Z
;
_time:2024-02:30
;
_time:2024-01-02:30
;
_time:2024-01-02+03:30
;
_time:2024-01-02T10+03:30
;
_time:2024-01-02T10:20+03:30
;
_time:2024-01-02T10:20:40+03:30
;
_time:2024-01-02T10:20:40-03:30
;
_time:"2024-01-02T10:20:40Z"
;
_time:2023-01-02T04:05:06.789Z
;
_time:2023-01-02T04:05:06.789-02:30
;
_time:2023-01-02T04:05:06.789+02:30
;
_time:=2023-01-02T04:05:06.789+02:30
;
_time:<2023-01-02T04:05:06.789+02:30
;
_time:>2023-01-02T04:05:06.789+02:30
;
_time:<=2023-01-02T04:05:06.789+02:30
;
_time:>=2023-01-02T04:05:06.789+02:30
;
_time:[1234567890, 1400000000]
;
_time:2d3h5.5m3s45ms
;
_time:=2d
;
_time:<2d
;
_time:<=2d
;
_time:>=2d
;
_time:>2d
;
_time:2023-01-05 OFFSET 5m
;
_time:[2023-01-05, 2023-01-06] OFFset 5m
;
_time:[2023-01-05, 2023-01-06) OFFset 5m
;
_time:(2023-01-05, 2023-01-06] OFFset 5m
;
_time:(2023-01-05, 2023-01-06) OFFset 5m
;
_time:1h offset 5.3m
;
_time:=1h offset 5.3m
;
_time:offset 1d
;
_time:offset -1.5d
;
_time:1h "offSet"
;
_time:1h (Offset)
;
_time:1h "and"
;
_time:1h _time:2025Z
;
_time:<10h _time:2025Z
;
_time:2025Z _time:[2024-10Z, 2025-03Z]
;
options(time_offset=1h) _time:2025Z _time:[2024-10Z, 2025-03Z]
;
_time:2025Z _time:2024Z _time:10y
;
_time:day_range[08:00, 20:30)
;
_time:day_range(08:00, 20:30)
;
_time:day_range(08:00, 20:30]
;
_time:day_range[08:00, 20:30]
;
_time:day_range[08:00, 20:30] offset 2.5h
;
_time:day_range[08:00, 20:30] offset -2.5h
;
_time:week_range[Mon, Fri]
;
_time:week_range(Monday, Friday] offset 2.5h
;
_time:week_range[monday, friday) offset -2.5h
;
_time:week_range(mon, fri]
;
and
;
and and or
;
AnD
;
or
;
`re` 'and' `or` 'not'
;
foo:and
;
"-"
;
"!"
;
"not"
;
''
;
eq_field
;
a:eq_field
;
le_field
;
a:le_field
;
lt_field
;
a:lt_field
;
exact
;
exact-foo
;
a:exact
;
a:exact-foo
;
i
;
i-foo
;
a:i-foo
;
"in"
;
`in-foo`
;
a:`in`
;
a:`in-foo`
;
ipv4_range
;
ipv4_range-foo
;
a:ipv4_range
;
a:ipv4_range-foo
;
ipv6_range
;
ipv6_range-foo
;
a:ipv6_range
;
a:ipv6_range-foo
;
len_range
;
len_range-foo
;
a:len_range
;
a:len_range-foo
;
pattern_match
;
pattern_match_full
;
pattern_match_prefix
;
pattern_match_suffix
;
a:pattern_match
;
a:pattern_match_full
;
a:pattern_match_prefix
;
a:pattern_match_suffix
;
range
;
range-foo
;
a:range
;
a:range-foo
;
re
;
re-bar
;
a:re-bar
;
seq
;
seq-a
;
x:seq-a
;
string_range
;
string_range-a
;
x:string_range-a
;
value_type
;
x:value_type
;
'options'
;
"options" foo
;
`options(x)`
;
_stream_id
;
x:_stream_id
;
_stream
;
x:_stream
;
_time
;
x:_time
;
contains_common_case(foo)
;
contains_common_case(foo, 'bar,baz')
;
foo:contains_common_case(foo, 'bar,baz')
;
equals_common_case(foo)
;
equals_common_case(foo, 'bar,baz')
;
foo:equals_common_case(foo, 'bar,baz')
;
eq_field(foo)
;
"a":eq_field('b')
;
-eq_field(a)
;
-a:eq_field(b)
;
a:!eq_field(b)
;
a:-eq_field(b)
;
le_field(foo)
;
"a":le_field('b')
;
-le_field(a)
;
-a:le_field(b)
;
a:!le_field(b)
;
a:-le_field(b)
;
lt_field(foo)
;
"a":lt_field('b')
;
-lt_field(a)
;
-a:lt_field(b)
;
a:!lt_field(b)
;
a:-lt_field(b)
;
exact(foo)
;
exact(foo*)
;
exact('foo bar),|baz')
;
exact('foo bar),|baz'*)
;
exact(foo/b:ar)
;
foo:exact(foo/b:ar*)
;
exact("foo/bar")
;
exact('foo/bar')
;
="foo/bar"
;
="foo=bar" !="b<=a>z" foo:!='abc'*
;
="=foo" =">=bar" x : ( = "=a<b"* ='c*' >=20)
;
i(foo)
;
i(foo*)
;
i(`foo`* )
;
i(' foo ) bar')
;
i('foo bar'*)
;
foo:i(foo:bar-baz/aa+bb)
;
json_array_contains_any()
;
abc:json_array_contains_any(foo, bar)
;
json_array_contains_any("foo")
;
json_array_contains_any('foo"')
;
in()
;
in(foo)
;
in(foo, bar)
;
in("foo bar", baz)
;
foo:in(foo-bar/baz)
;
in(*)
;
foo:in(*)
;
in(err|fields x)
;
ip:in(foo and user:in(admin, moderator)|fields ip)
;
in(bar:in(1,2,3) | uniq (x)) | stats count() rows
;
in((1) | fields z) | stats count() rows
;
contains_any()
;
contains_any(foo)
;
contains_any(foo, bar)
;
contains_any("foo bar", baz)
;
foo:contains_any(foo-bar/baz)
;
contains_any(*)
;
foo:contains_any(*)
;
contains_any(err|fields x)
;
ip:contains_any(foo and user:contains_any(admin, moderator)|fields ip)
;
contains_any(bar:contains_any(1,2,3) | uniq (x)) | stats count() rows
;
contains_any((1) | fields z) | stats count() rows
;
contains_all()
;
contains_all(foo)
;
contains_all(foo, bar)
;
contains_all("foo bar", baz)
;
foo:contains_all(foo-bar/baz)
;
contains_all(*)
;
foo:contains_all(*)
;
contains_all(err|fields x)
;
ip:contains_all(foo and user:contains_all(admin, moderator)|fields ip)
;
contains_all(bar:contains_all(1,2,3) | uniq (x)) | stats count() rows
;
contains_all((1) | fields z) | stats count() rows
;
ipv4_range(1.2.3.4, "5.6.7.8")
;
foo:ipv4_range(1.2.3.4, "5.6.7.8" , )
;
ipv4_range(1.2.3.4)
;
ipv4_range(1.2.3.4/20)
;
ipv4_range(1.2.3.4,)
;
ipv6_range(::1, "::2")
;
len_range(10, 20)
;
foo:len_range("10", 20, )
;
len_RANGe(10, inf)
;
len_range(10, +InF)
;
len_range(10, 1_000_000)
;
len_range(0x10,0b100101)
;
len_range(1.5KB, 22MB100KB)
;
pattern_match("<N> foo <DATE>, bar")
;
pattern_match_full("<N> foo <DATE>, bar")
;
pattern_match_prefix("<N> foo <DATE>, bar")
;
pattern_match_suffix("<N> foo <DATE>, bar")
;
range(-INF,+inF)
;
foo: > 10.5M
;
foo: >= 10.5M
;
foo: < 10.5M
;
foo: <= 10.5M
;
re('foo|ba(r.+)')
;
re(foo)
;
foo:re(foo-bar/baz.)
;
~foo.bar.baz !~bar
;
foo:~"~foo~ba/ba>z"
;
foo:~'.*'
;
foo:~'.+'
;
~".*"
;
~".+"
;
foo bar:~".*"
;
foo bar:~""
;
foo bar:~".+"
;
x:~".*"
;
x:~"a*"
;
~'a*'
;
seq()
;
seq(foo)
;
seq("foo, bar", baz, abc)
;
foo:seq(foo,bar-baz+aa, b)
;
string_range(foo, bar)
;
foo:string_range("foo, bar", baz)
;
foo:>bar
;
foo:>"1234"
;
>="abc"
;
foo:<bar
;
foo:<"-12.34"
;
<="abc < de"
;
"_stream"
;
"_time"
;
"_msg"
;
_stream and _time or _msg
;
1.2.3.4 or ip:5.6.7.9
;
foo-bar+baz*
;
foo- bar
;
foo -bar
;
`foo!bar`
;
foo:`aa!bb:cc`
;
foo:bar:baz
;
foo:(bar baz:xxx)
;
foo:(_time:abc or not z)
;
foo:(_msg:a :x '_stream:{c="d"}')
;
(_msg:a:b c)
;
'"foo"bar' baz:"a'b'c"
;
foo|fields *
;
foo | fields bar
;
foo | fields "", a
;
* | field_values x
;
* | field_values (x)
;
foo | generate_sequence 123
;
* | copy foo as bar
;
* | cp foo bar
;
* | COPY foo as bar, x y | Copy a as b
;
* | rename foo as bar
;
* | mv foo bar
;
* | RENAME foo AS bar, x y | Rename a as b
;
* | delete foo
;
* | del foo
;
* | rm foo
;
* | DELETE foo, bar
;
* | len(x)
;
* | len(x) as _msg
;
* | len(x) y
;
* | len  ( x ) as y
;
* | len x y
;
* | len x as y
;
foo | limit
;
foo | head
;
foo | limit 20
;
foo | head 20
;
foo | HEAD 1_123_432
;
foo | head 10K
;
foo | limit 100 | limit 10 | limit 234
;
foo | skip 10
;
foo | offset 10
;
foo | skip 12_345M
;
foo | offset 10 | offset 100
;
* | sample 10
;
* | running_stats count() x
;
* | total_stats count() x
;
* | split ","
;
* | split "," from x
;
* | split "," from x as y
;
* | split "," as y
;
* | stats count() x
;
* | stats_remote count() x
;
* | stats count(*) x
;
* | stats count('') foo
;
* | stats count(foo) ''
;
* | count()
;
* | count(), count() if (foo)
;
* | stats by (x, y) count_empty(a,b,c) z
;
* | stats Max(foo) bar
;
* | stats BY(x, y, ) MAX(foo,bar,) bar
;
* | stats Min(foo) bar
;
* | stats BY(x, y, ) MIN(foo,bar,) bar
;
* | stats BY(x, y, ) row_MIN(foo,bar,) bar
;
* | stats field_Max(foo,x) bar
;
* | field_Max(foo,x)
;
* | stats BY(x, y, ) field_MAX(foo,bar) bar
;
* | stats field_Min(foo,x) bar
;
* | field_Min(foo,x)
;
* | stats BY(x, y, ) field_MIN(foo,bar) bar
;
* | stats Any(foo) bar
;
* | stats BY(x, y, ) ANY(foo) bar
;
* | stats count_uniq(foo) bar
;
* | count_uniq(foo)
;
* | stats by(x, y) count_uniq(foo,bar) LiMit 10 As baz
;
* | stats by(x) count_uniq(y) z
;
* | stats by(x) count_uniq(a,b) z
;
* | stats uniq_values(foo) bar
;
* | uniq_values(foo)
;
* | stats uniq_values(foo) limit 10 bar
;
* | stats values(foo) bar
;
* | values(foo)
;
* | stats values(foo) limit 10 bar
;
* | stats Sum_len(foo) bar
;
* | stats BY(x, y, ) SUM_Len(foo,bar,) bar
;
* | json_values(a) x
;
* | json_values(a, b) limit 5 x
;
* | stats count() "foo.bar:baz", count_uniq(a) bar
;
* | stats by (x, y) count(*) foo, count_uniq(a,b) bar
;
*|stats by(client_ip:/24, server_ip:/16) count() foo
;
* | stats by(_time: 1d offset 2h) count() as foo
;
* | stats by(_time:1d offset -2.5h5m) count() as foo
;
* | stats by (_time:nanosecond) count() foo
;
* | stats by (_time:microsecond) count() foo
;
* | stats by (_time:millisecond) count() foo
;
* | stats by (_time:second) count() foo
;
* | stats by (_time:minute) count() foo
;
* | stats by (_time:hour) count() foo
;
* | stats by (_time:day) count() foo
;
* | stats by (_time:week) count() foo
;
* | stats by (_time:month) count() foo
;
* | stats by (_time:year offset 6.5h) count() foo
;
* | stats (_time:year offset 6.5h) count() foo
;
* | stats count() if (foo bar) rows
;
* | stats count(x) if (error ip:in(_time:1d | fields ip)) rows
;
* | stats count() if () rows
;
* | sort
;
* | order
;
* | sort desc
;
* | sort by()
;
* | sort bY (foo)
;
* | ORDer bY (foo)
;
* | sort bY (foo desc, bar,) desc
;
* | sort limit 10
;
* | sort offset 20 limit 10
;
* | sort desc limit 10
;
* | sort desc offset 20 limit 10
;
* | sort by (foo desc, bar) limit 10
;
* | sort by (foo desc, bar) oFFset 20 limit 10
;
* | sort by (foo desc, bar) desc limit 10
;
* | sort by (foo desc, bar) desc OFFSET 30 limit 10
;
* | sort by (foo desc, bar) desc limit 10 OFFSET 30
;
* | sort (foo desc, bar) desc limit 10 OFFSET 30
;
* | first
;
* | first by (x,y)
;
* | first 10 by (foo)
;
* | first 10 by (foo) rank bar
;
* | last
;
* | last by (x,y)
;
* | last 10 by (foo)
;
* | last 10 by (foo) rank bar
;
* | uniq foo
;
* | uniq foo,bar
;
* | uniq by(f1,f2)
;
* | uniq by(f1,f2) limit 10
;
* | uniq (f1,f2) limit 10
;
* | filter error ip:12.3.4.5 or warn
;
foo | stats by (host) count() logs | filter logs:>50 | sort by (logs desc) | limit 10
;
* | "error"
;
* | filter error
;
* | -unpack_logfmt
;
* |~foo
;
* | "by"
;
* | "stats" *
;
* | * "count"
;
* | !foo
;
* | !~"re"
;
* | !=foo
;
* | =foo
;
* | {host="x"}
;
* | not foo
;
* | NOT foo
;
* | not foo:bar
;
* | not (foo:bar)
;
* | not not foo
;
* | not !foo
;
* | not foo or bar
;
* | not foo and bar
;
* | "not"
;
* | extract "foo<bar>baz"
;
* | extract "foo<bar>baz" from _msg
;
* | extract 'foo<bar>baz' from ''
;
* | extract `foo<bar>baz` from x
;
* | extract if (a:b) 'foo<bar>baz' from x
;
* | union(foo)
;
* | join by (x) (foo:bar)
;
* | join on (x, y) (foo:bar)
;
* | join (x, y) (foo:bar)
;
* | json_array_concat
;
* | json_array_concat "," x
;
* | json_array_concat "," from x
;
* | json_array_concat "," as y
;
* | json_array_concat "," from x as y
;
* | json_array_len x
;
* | json_array_len x y
;
* | json_array_len (x) as y
;
* | unpack_words
;
* | unpack_words x
;
* | unpack_words x y
;
* | hash(x)
;
* | hash(x) y
;
* | hash(x) as y
;
* | skip 100 | head 20 | skip 10
;
* | by (host) count() rows | rows:>10
;
* | (host) count() rows, count() if (error) errors | rows:>10
;
options () foo
;
options (concurrency=10) foo | count() c
;
options (concurrency=10, concurrency =   42,) foo | count() c
;
options (concurrency=0) *
;
options (concurrency=1) *
;
options (parallel_readers=10) *
;
options(ignore_global_time_filter=true) *
;
options(time_offset=1h) *
;
options(time_offset=1h) _time:1d
;
options(global_filter=(*)) *
;
options(global_filter=(_time:5m)) foo:bar
;
options(global_filter=(_time:5m {host="abc"})) _time:1h foo:in(_time:3m | keep foo)
;
options (concurrency=2) foo bar:in(a:b | uniq(bar)) | union (abc) | join on (x) (y)
;
options (concurrency=2) foo bar:in(options (concurrency=10, ignore_global_time_filter=true) a:b | uniq(bar)) | union (abc) | join on(x) (y)
;
options (concurrency=2) foo bar:contains_any(a:b | uniq(bar)) | union (abc) | join on (x) (y)
;
options (concurrency=2) foo bar:contains_any(options (concurrency=10, ignore_global_time_filter=true) a:b | uniq(bar)) | union (abc) | join on(x) (y)
;
options (concurrency=2) foo bar:contains_all(a:b | uniq(bar)) | union (abc) | join on (x) (y)
;
options (concurrency=2) foo bar:contains_all(options (concurrency=10, ignore_global_time_filter=true) a:b | uniq(bar)) | union (abc) | join on(x) (y)
;
foo x:in(bar | filter baz | sort (a) | offset 10 | limit 20 | keep x)
;
foo x:contains_any(bar | filter baz | sort (a) | offset 10 | limit 20 | keep x)
;
foo x:contains_all(bar | filter baz | sort (a) | offset 10 | limit 20 | keep x)
;
* | join (x) ({foo=bar} {baz=x}) | count() if (a:in((a b) c (d e) | keep a)) z
;
* | join (x) ({foo=bar} {baz=x}) | count() if (a:contains_any((a b) c (d e) | keep a)) z
;
* | join (x) ({foo=bar} {baz=x}) | count() if (a:contains_all((a b) c (d e) | keep a)) z
;
(a)or(b)
;
_time:[5m, 10m]OR not(x)
;
(a)and(b)
;
_time:[5m, 10m]ANd not(x)
;
*|* (foo)
;
a:(foo)
;
((a))
;
!(a)
;
-(a)
;
not(a)
;
a and(b)
;
a or(b)
;
a|foo:bar
;
a|~b
;
a:~b
;
(~a)
;
!~a
;
-~a
;
foo-bar.baz/x+z$
;
foo:bar:baz:x-z
;
filter foo:bar
;
stats count
;
count
;
fields.foo
;
foo | filter stats
;
foo | limit 1 | filter stats
;
foo | filter fields
;
foo | filter by
;
foo | filter count
;
foo | * bar
;
foo | -bar
;
a * foo
CORPUS_EOF
$CLICKHOUSE_CLIENT "${LOGSQL_OPTS[@]}" --queries-file "$VALID_QUERIES" < /dev/null > /dev/null && echo "valid queries: OK"

# All these queries are invalid in LogsQL and must be rejected.
# The queries are sent over HTTP, because spawning a separate client for each of them is too slow under sanitizers.
LOGSQL_URL="${CLICKHOUSE_URL}&dialect=logsql&allow_experimental_logsql_dialect=1&logsql_table=corpus_logs_04616"
while IFS= read -r query; do
    if ! ${CLICKHOUSE_CURL} -sS "$LOGSQL_URL" --data-binary "$query" 2>&1 | grep -q "Code:"; then
        echo "UNEXPECTEDLY ACCEPTED: $query"
    fi
done <<'INVALID_EOF'
|
a and;
:foo
foo !~ bar
foo > bar
1*1
~(bar)
1 ~~ 2
:  
_stream_id:()
_stream:(
_stream:{foo=bar,
{foo=
_time:[foo,bar)
_time:[2023-01-02T04:05:06-12,2023]
_time:day_range[00:00,
_time:week_range[Sun,mom]
foo ]bar
eq_field(foo, bar)
lt_field(
exact(foo, bar
json_array_contains_any(
in(foo, "bar baz"*)
contains_any(
contains_any(foo|bar)
contains_all(foo,
ipv4_range(
ipv4_range(1.2.3.4, 5.6.7.8,5.3.2.1)
pattern_match_full(
range(1,)
re(a, b)
seq(foo*)
string_range(foo, bar
foo | fields bar,,
foo | block_stats foo
foo | blocks_count x y
foo | collapse_nums bar
foo | rename foo,,
foo | limit bar
foo | running_stats
foo | stats count
foo | stats max
foo | stats field_min
foo | stats uniq_values
foo | stats sum_len
foo | stats quantile(10, x) foo
foo | stats by(bar,
foo | sort by(bar) foo
foo | sort by(bar) offset 10 offset 20
foo | filter | sort by (x)
foo | by
foo | extract from x
foo | unpack_json from
options(
options(concurrency=12)
* | extract.x from y
* | join.by(x) (y)
* | pack_logfmt.x
* | stream_context.before 10
* | unroll.x
* | unique by (_msg) limit 1
INVALID_EOF
echo "invalid queries: OK"

$CLICKHOUSE_CLIENT -q "DROP TABLE corpus_logs_04616"
