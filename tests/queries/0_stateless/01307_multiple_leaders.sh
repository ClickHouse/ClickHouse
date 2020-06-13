#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -n --query "
DROP TABLE IF EXISTS r0;
DROP TABLE IF EXISTS r1;

CREATE TABLE r0 (x UInt64) ENGINE = ReplicatedMergeTree('/test/table', 'r0') ORDER BY x SETTINGS min_bytes_for_wide_part = '10M';
CREATE TABLE r1 (x UInt64) ENGINE = ReplicatedMergeTree('/test/table', 'r1') ORDER BY x SETTINGS min_bytes_for_wide_part = '10M';
"

function thread()
{
    REPLICA=$1
    ITERATIONS=$2

    $CLICKHOUSE_CLIENT --max_block_size 1 --min_insert_block_size_rows 0 --min_insert_block_size_bytes 0 --query "INSERT INTO r$REPLICA SELECT number * 2 + $REPLICA FROM numbers($ITERATIONS)"
}


thread 0 1000 &
thread 1 1000 &

wait

$CLICKHOUSE_CLIENT -n --query "
SYSTEM SYNC REPLICA r0;
SYSTEM SYNC REPLICA r1;

SELECT count(), sum(x) FROM r0;
SELECT count(), sum(x) FROM r1;

DROP TABLE r0;
DROP TABLE r1;
"
