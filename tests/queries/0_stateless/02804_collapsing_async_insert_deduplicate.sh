#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists 02804_collapsing_async_insert_deduplicate sync"
$CLICKHOUSE_CLIENT -q "create table 02804_collapsing_async_insert_deduplicate (k Int64, s Int8) engine ReplicatedCollapsingMergeTree('/clickhouse/tables/{database}/02804_collapsing_async_insert_deduplicate', 'r1', s) order by k"

SETTINGS="async_insert=1, async_insert_deduplicate=1, wait_for_async_insert=0, optimize_on_insert=1"
# Data to insert: " (1,1)" repeated many times, then (1,-1) repeated many times.
VALUES_1=`yes " (1,1)" | head -n701 | tr -d '\n'`
VALUES_2=`yes " (1,-1)" | head -n700 | tr -d '\n'`

# Spam these queries a bunch of times. This used to crash.
for i in {1..30}
do
    $CLICKHOUSE_CLIENT -q "insert into 02804_collapsing_async_insert_deduplicate settings $SETTINGS values$VALUES_1"
    $CLICKHOUSE_CLIENT -q "insert into 02804_collapsing_async_insert_deduplicate settings $SETTINGS values$VALUES_2"
done

# Wait.
$CLICKHOUSE_CLIENT -q "insert into 02804_collapsing_async_insert_deduplicate settings async_insert=1, async_insert_deduplicate=1, wait_for_async_insert=1 values (0,1)"

$CLICKHOUSE_CLIENT -q "select count() from 02804_collapsing_async_insert_deduplicate final"
