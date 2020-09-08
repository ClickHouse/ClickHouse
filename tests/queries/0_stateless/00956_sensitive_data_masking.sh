#!/usr/bin/env bash

# Get all server logs
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL="trace"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

cur_name=$(basename "${BASH_SOURCE[0]}")
tmp_file=${CLICKHOUSE_TMP}/$cur_name"_server.logs"
tmp_file2=${CLICKHOUSE_TMP}/$cur_name"_server.2.logs"

rm -f "$tmp_file" >/dev/null 2>&1
echo 1
# normal execution
$CLICKHOUSE_CLIENT \
  --query="SELECT 'find_me_TOPSECRET=TOPSECRET' FROM numbers(1) FORMAT Null" \
  --log_queries=1 --ignore-error --multiquery >"$tmp_file" 2>&1

grep -F 'find_me_[hidden]' "$tmp_file" >/dev/null || echo 'fail 1a'
grep -F 'TOPSECRET' "$tmp_file" && echo 'fail 1b'

rm -f "$tmp_file" >/dev/null 2>&1
echo 2
# failure at parsing stage
echo "SELECT 'find_me_TOPSECRET=TOPSECRET' FRRRROM numbers" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d @- >"$tmp_file" 2>&1

#cat $tmp_file

## can't be checked on client side!
# grep -F 'find_me_[hidden]' $tmp_file >/dev/null || echo 'fail 2a'
grep -F 'TOPSECRET' "$tmp_file" && echo 'fail 2b'

rm -f "$tmp_file" >/dev/null 2>&1
echo 3
# failure at before query start
$CLICKHOUSE_CLIENT \
  --query="SELECT 'find_me_TOPSECRET=TOPSECRET' FROM non_existing_table FORMAT Null" \
  --log_queries=1 --ignore-error --multiquery >"$tmp_file" 2>&1

grep -F 'find_me_[hidden]' "$tmp_file" >/dev/null || echo 'fail 3a'
grep -F 'TOPSECRET' "$tmp_file" && echo 'fail 3b'

rm -f "$tmp_file" >/dev/null 2>&1
echo 4
# failure at the end of query
$CLICKHOUSE_CLIENT \
  --query="SELECT 'find_me_TOPSECRET=TOPSECRET', intDiv( 100, number - 10) FROM numbers(11) FORMAT Null" \
  --log_queries=1 --ignore-error --max_block_size=2 --multiquery >"$tmp_file" 2>&1

grep -F 'find_me_[hidden]' "$tmp_file" >/dev/null || echo 'fail 4a'
grep -F 'TOPSECRET' "$tmp_file" && echo 'fail 4b'

echo 5
# run in background
rm -f "$tmp_file2" >/dev/null 2>&1
bash -c "$CLICKHOUSE_CLIENT \
  --query=\"select sleepEachRow(1) from numbers(10) where ignore('find_me_TOPSECRET=TOPSECRET')=0 and ignore('fwerkh_that_magic_string_make_me_unique') = 0 FORMAT Null\" \
  --log_queries=1 --ignore-error --multiquery >$tmp_file2 2>&1" &

rm -f "$tmp_file" >/dev/null 2>&1
# check that executing query doesn't expose secrets in processlist
echo '5.1'

# wait until the query in background will start (max: 10 seconds as sleepEachRow)
for _ in {1..100}; do
    $CLICKHOUSE_CLIENT --query="SHOW PROCESSLIST" --log_queries=0 >"$tmp_file" 2>&1
    grep -q -F 'fwerkh_that_magic_string_make_me_unique' "$tmp_file" && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query="KILL QUERY WHERE query LIKE '%fwerkh_that_magic_string_make_me_unique%'" > /dev/null 2>&1
wait
grep 'TOPSECRET' "$tmp_file2" && echo 'fail 5d'

rm -f "$tmp_file2" >/dev/null 2>&1

grep -F 'fwerkh_that_magic_string_make_me_unique' "$tmp_file" >"$tmp_file2" || echo 'fail 5a'
grep -F 'find_me_[hidden]' "$tmp_file2" >/dev/null  || echo 'fail 5b'
grep -F 'TOPSECRET' "$tmp_file" && echo 'fail 5c'


# instead of disabling send_logs_level=trace (enabled globally for that test) - redir it's output to /dev/null
$CLICKHOUSE_CLIENT \
  --server_logs_file=/dev/null \
  --query="system flush logs"


echo 6
# check events count properly increments
$CLICKHOUSE_CLIENT \
  --server_logs_file=/dev/null \
  --query="select * from (select sum(value) as matches from system.events where event='QueryMaskingRulesMatch') where matches < 5"

echo 7
# and finally querylog
$CLICKHOUSE_CLIENT \
  --server_logs_file=/dev/null \
  --query="select * from system.query_log where event_time > now() - 10 and query like '%TOPSECRET%';"


rm -f "$tmp_file" >/dev/null 2>&1
echo 8
$CLICKHOUSE_CLIENT \
   --query="drop table if exists sensetive; create table sensitive ( id UInt64, date Date, value1 String, value2 UInt64) Engine=MergeTree ORDER BY id PARTITION BY date;
insert into sensitive select number as id, toDate('2019-01-01') as date, 'abcd' as value1, rand() as valuer from numbers(10000);
insert into sensitive select number as id, toDate('2019-01-01') as date, 'find_me_TOPSECRET=TOPSECRET' as value1, rand() as valuer from numbers(10);
insert into sensitive select number as id, toDate('2019-01-01') as date, 'abcd' as value1, rand() as valuer from numbers(10000);
select * from sensitive WHERE value1 = 'find_me_TOPSECRET=TOPSECRET' FORMAT Null;
drop table sensitive;" --log_queries=1 --ignore-error --multiquery >"$tmp_file" 2>&1

grep -F 'find_me_[hidden]' "$tmp_file" >/dev/null || echo 'fail 8a'
grep -F 'TOPSECRET' "$tmp_file" && echo 'fail 8b'

$CLICKHOUSE_CLIENT --query="SYSTEM FLUSH LOGS" --server_logs_file=/dev/null

echo 9
$CLICKHOUSE_CLIENT \
   --server_logs_file=/dev/null \
   --query="SELECT if( count() > 0, 'text_log non empty', 'text_log empty') FROM system.text_log WHERE event_time>now() - 60 and message like '%find_me%';
   select * from system.text_log where event_time > now() - 60 and message like '%TOPSECRET=TOPSECRET%';"  --ignore-error --multiquery

echo 'finish'
rm -f "$tmp_file" >/dev/null 2>&1
rm -f "$tmp_file2" >/dev/null 2>&1
