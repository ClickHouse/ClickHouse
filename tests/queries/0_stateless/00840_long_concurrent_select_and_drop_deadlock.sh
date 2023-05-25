#!/usr/bin/env bash
# Tags: deadlock, no-parallel

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function cleanup()
{
    echo Failed
    wait
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "create view view_00840 as select count(*),database,table from system.columns group by database,table"

for _ in {1..100}; do
    $CLICKHOUSE_CLIENT -nm -q "
        drop table if exists view_00840;
        create view view_00840 as select count(*),database,table from system.columns group by database,table;
    "
done &
for _ in {1..250}; do
    $CLICKHOUSE_CLIENT -q "select * from view_00840 order by table" >/dev/null 2>&1 || true
done &

wait
trap '' EXIT

echo "drop table view_00840" | $CLICKHOUSE_CLIENT

echo 'did not deadlock'
