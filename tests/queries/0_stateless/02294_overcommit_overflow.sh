#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q 'DROP USER IF EXISTS u02294'
$CLICKHOUSE_CLIENT -q 'CREATE USER IF NOT EXISTS u02294 IDENTIFIED WITH no_password'
$CLICKHOUSE_CLIENT -q 'GRANT ALL ON *.* TO u02294'

function query()
{
  while true; do
    $CLICKHOUSE_CLIENT -u u02294 -q 'SELECT number FROM numbers(130000) GROUP BY number SETTINGS max_memory_usage_for_user=5000000,memory_overcommit_ratio_denominator=2000000000000000000,memory_usage_overcommit_max_wait_microseconds=500' >/dev/null 2>/dev/null
  done
}

export -f query

TIMEOUT=10

for _ in {1..10};
do
    timeout $TIMEOUT bash -c query &
done

wait

$CLICKHOUSE_CLIENT -q 'DROP USER IF EXISTS u02294'
