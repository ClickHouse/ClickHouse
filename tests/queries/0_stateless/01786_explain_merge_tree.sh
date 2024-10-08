#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for i in $(seq 0 1)
do
    CH_CLIENT="$CLICKHOUSE_CLIENT --optimize_move_to_prewhere=1 --convert_query_to_cnf=0 --optimize_read_in_order=1 --enable_analyzer=$i"

    $CH_CLIENT -q "drop table if exists test_index"
    $CH_CLIENT -q "drop table if exists idx"

    $CH_CLIENT -q "create table test_index (x UInt32, y UInt32, z UInt32, t UInt32, index t_minmax t % 20 TYPE minmax GRANULARITY 2, index t_set t % 19 type set(4) granularity 2) engine = MergeTree order by (x, y) partition by (y, bitAnd(z, 3), intDiv(t, 15)) settings index_granularity = 2, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1"
    $CH_CLIENT -q "insert into test_index select number, number > 3 ? 3 : number, number = 1 ? 1 : 0, number from numbers(20)"

    $CH_CLIENT -q "
        explain indexes = 1 select *, _part from test_index where t % 19 = 16 and y > 0 and bitAnd(z, 3) != 1 and x > 10 and t % 20 > 14;
        " | grep -A 100 "ReadFromMergeTree" | grep -v "Condition"

    echo "-----------------"

    $CH_CLIENT -q "
        explain indexes = 1, json = 1 select *, _part from test_index where t % 19 = 16 and y > 0 and bitAnd(z, 3) != 1 and x > 10 and t % 20 > 14 format TSVRaw;
        " | grep -A 100 "ReadFromMergeTree" | grep -v "Condition"

    echo "-----------------"

    $CH_CLIENT -q "
        explain actions = 1 select x from test_index where x > 15 order by x;
        " | grep -A 100 "ReadFromMergeTree"

    echo "-----------------"

    $CH_CLIENT -q "
        explain actions = 1 select x from test_index where x > 15 order by x desc;
        " | grep -A 100 "ReadFromMergeTree"

    $CH_CLIENT -q "CREATE TABLE idx (x UInt32, y UInt32, z UInt32) ENGINE = MergeTree ORDER BY (x, x + y) settings min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1"
    $CH_CLIENT -q "insert into idx select number, number, number from numbers(10)"

    $CH_CLIENT -q "
        explain indexes = 1 select z from idx where not(x + y + 1 > 2 and x not in (4, 5))
        " | grep -A 100 "ReadFromMergeTree"

    $CH_CLIENT -q "drop table if exists test_index"
    $CH_CLIENT -q "drop table if exists idx"
done
