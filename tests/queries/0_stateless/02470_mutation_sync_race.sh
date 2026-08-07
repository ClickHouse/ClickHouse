#!/usr/bin/env bash
# Tags: long, zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "drop table if exists src;"
$CLICKHOUSE_CLIENT -q "create table src(A UInt64) Engine=ReplicatedMergeTree('/clickhouse/{database}/test/src1', '1') order by tuple() SETTINGS min_bytes_for_wide_part=0;"
$CLICKHOUSE_CLIENT -q "insert into src values (0)"

function thread()
{
    local TIMELIMIT=$((SECONDS+$1))
    for i in $(seq 1000); do
        if [ $SECONDS -ge "$TIMELIMIT" ]; then
            return
        fi
        $CLICKHOUSE_CLIENT -q "alter table src detach partition tuple()"
        $CLICKHOUSE_CLIENT -q "alter table src attach partition tuple()"
        $CLICKHOUSE_CLIENT -q "alter table src update A = ${i} where 1 settings mutations_sync=2"
        $CLICKHOUSE_CLIENT -q "select throwIf(A != ${i}) from src format Null"
    done
}

TIMEOUT=30

thread $TIMEOUT || true