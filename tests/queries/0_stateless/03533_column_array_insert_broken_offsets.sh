#!/usr/bin/env bash
# Refs: https://github.com/ClickHouse/ClickHouse/issues/81199

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT 'create table array_deserialize_offsets (val Array(String)) engine=Memory()'

{
  # Native header
  $CLICKHOUSE_LOCAL "select * from values('val Array(String)', (['foo','fooo']),(['bar','barr'])) format Native" | head -c20
  # Array offsets (without header) that are decreasing (2, 1)
  $CLICKHOUSE_LOCAL "select * from values('val UInt64', (2),(1)) format Native" | tail -c+14
  # We do not even need array values, offsets should be checked before reading them
} | $CLICKHOUSE_CLIENT 'insert into array_deserialize_offsets format Native' |& grep -o 'Arrays offsets are not monotonically increasing (starting at 0, value 2).*INCORRECT_DATA'

{
  # Native header
  $CLICKHOUSE_LOCAL "select * from values('val Array(String)', (['foo','fooo']),(['bar','barr'])) format Native" | head -c20
  # Monotonic offsets (1, 2) followed by a single value, so one value is missing
  $CLICKHOUSE_LOCAL "select * from values('val UInt64', (1),(2)) format Native" | tail -c+14
  printf '\x03foo'
} | $CLICKHOUSE_CLIENT 'insert into array_deserialize_offsets format Native' |& grep -o 'Cannot read all array values: read just 1 of 2.*CANNOT_READ_ALL_DATA'

{
  # Native header
  $CLICKHOUSE_LOCAL "select * from values('val Array(String)', (['foo','fooo']),(['bar','barr'])) format Native" | head -c20
  # Monotonic offsets (1, 2) with no values at all
  $CLICKHOUSE_LOCAL "select * from values('val UInt64', (1),(2)) format Native" | tail -c+14
} | $CLICKHOUSE_CLIENT 'insert into array_deserialize_offsets format Native' |& grep -o 'Cannot read array values: elements column is empty while the last offset is 2.*INCORRECT_DATA'

$CLICKHOUSE_CLIENT 'select count() from array_deserialize_offsets'
