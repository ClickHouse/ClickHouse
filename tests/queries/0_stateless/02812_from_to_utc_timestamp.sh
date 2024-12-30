#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


client_opts=(
  --session_timezone Europe/Moscow
)

$CLICKHOUSE_CLIENT -q "drop table if exists test_tbl"
$CLICKHOUSE_CLIENT -q "create table test_tbl (x UInt32, y DateTime, z DateTime64) engine=MergeTree order by x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl values(1, '2023-03-16', '2023-03-16 11:22:33')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl values(2, '2023-03-16 11:22:33', '2023-03-16')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO test_tbl values(3, '2023-03-16 11:22:33', '2023-03-16 11:22:33.123456')"
$CLICKHOUSE_CLIENT -q "select x, to_utc_timestamp(toDateTime('2023-03-16 11:22:33'), 'Etc/GMT+1'), from_utc_timestamp(toDateTime64('2023-03-16 11:22:33', 3), 'Etc/GMT+1'), to_utc_timestamp(y, 'Asia/Shanghai'), from_utc_timestamp(z, 'Asia/Shanghai') from test_tbl order by x"
# timestamp convert between DST timezone and UTC
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select to_utc_timestamp(toDateTime('2024-02-24 11:22:33'), 'Europe/Madrid'), from_utc_timestamp(toDateTime('2024-02-24 11:22:33'), 'Europe/Madrid')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select to_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'Europe/Madrid'), from_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'Europe/Madrid')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select to_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'EST'), from_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'EST')"

$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select 'leap year:', to_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'EST'), from_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'EST')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select 'non-leap year:', to_utc_timestamp(toDateTime('2023-02-29 11:22:33'), 'EST'), from_utc_timestamp(toDateTime('2023-02-29 11:22:33'), 'EST')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select 'leap year:', to_utc_timestamp(toDateTime('2024-02-28 23:22:33'), 'EST'), from_utc_timestamp(toDateTime('2024-03-01 00:22:33'), 'EST')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select 'non-leap year:', to_utc_timestamp(toDateTime('2023-02-28 23:22:33'), 'EST'), from_utc_timestamp(toDateTime('2023-03-01 00:22:33'), 'EST')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select 'timezone with half-hour offset:', to_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'Australia/Adelaide'), from_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'Australia/Adelaide')"
$CLICKHOUSE_CLIENT "${client_opts[@]}" -q "select 'jump over a year:', to_utc_timestamp(toDateTime('2023-12-31 23:01:01'), 'EST'), from_utc_timestamp(toDateTime('2024-01-01 01:01:01'), 'EST')"

$CLICKHOUSE_CLIENT -q "drop table test_tbl"
