#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS r1;
    DROP TABLE IF EXISTS r2;

    CREATE TABLE r1 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/r', 'r1') ORDER BY x;
    CREATE TABLE r2 (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/r', 'r2') ORDER BY x;
"

function thread {
    for x in {0..99}; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO r$1 SELECT $x"
    done
}

thread 1 &
thread 2 &

wait

$CLICKHOUSE_CLIENT -n -q "
    SYSTEM SYNC REPLICA r1;
    SYSTEM SYNC REPLICA r2;
"

$CLICKHOUSE_CLIENT -q "SELECT count(), min(x), max(x), sum(x) FROM r1";
$CLICKHOUSE_CLIENT -q "SELECT count(), min(x), max(x), sum(x) FROM r2";

$CLICKHOUSE_CLIENT -n -q "
    DROP TABLE IF EXISTS r1;
    DROP TABLE IF EXISTS r2;
"
