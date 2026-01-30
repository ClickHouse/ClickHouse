#!/usr/bin/env bash
# Tags: race

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS alter_table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE alter_table (a UInt8, b UInt8, c UInt8, d UInt8, e UInt8, f UInt8, g UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/default/test_03518/alter_table', 'r1') ORDER BY a PARTITION BY b % 10 SETTINGS old_parts_lifetime = 1"

function thread_alter()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        ERROR=$($CLICKHOUSE_CLIENT --query "
            ALTER TABLE alter_table ADD COLUMN $1 String DEFAULT '0';
            ALTER TABLE alter_table MODIFY COLUMN $1 UInt64;
            ALTER TABLE alter_table DROP COLUMN $1;" 2>&1 | tr '\n' ' ')

        if [[ ! "${ERROR}" =~ "You can retry this error" ]]
        then
            echo "${ERROR}"
        fi
    done
}

function thread_insert()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT -q "INSERT INTO alter_table (a, b, c, d, e, f, g) SELECT rand(1), rand(2), rand(3), rand(4), rand(5), rand(6), rand(7) FROM numbers(100000)"
    done
}

TIMEOUT=30

thread_alter h &
thread_insert &

thread_alter i &
thread_insert &

thread_alter j &
thread_insert &

thread_alter k &
thread_insert &


wait

$CLICKHOUSE_CLIENT -q "DROP TABLE alter_table"
