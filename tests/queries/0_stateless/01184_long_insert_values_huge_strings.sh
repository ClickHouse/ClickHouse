#!/usr/bin/env bash
# Tags: long, no-tsan
# FIXME It became flaky after upgrading to llvm-14 due to obscure freezes in tsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists huge_strings"
$CLICKHOUSE_CLIENT -q "create table huge_strings (n UInt64, l UInt64, s String, h UInt64) engine=MergeTree order by n"

# Timeouts are increased, because test can be slow with sanitizers and parallel runs.

for _ in {1..10}; do
  $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "select number, (rand() % 10*1000*1000) as l, repeat(randomString(l/1000/1000), 1000*1000) as s, cityHash64(s) from numbers(10) format Values" | $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "insert into huge_strings values" &
  $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "select number % 10, (rand() % 10) as l, randomString(l) as s, cityHash64(s) from numbers(100000)" | $CLICKHOUSE_CLIENT --receive_timeout 100 --send_timeout 100 --connect_timeout 100 --query "insert into huge_strings format TSV" &
done;
wait

$CLICKHOUSE_CLIENT -q "select count() from huge_strings"
$CLICKHOUSE_CLIENT -q "select sum(l = length(s)) from huge_strings"
$CLICKHOUSE_CLIENT -q "select sum(h = cityHash64(s)) from huge_strings"

$CLICKHOUSE_CLIENT -q "drop table huge_strings"
