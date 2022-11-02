#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

test_random_values() {
  layers=$1
  $CLICKHOUSE_CLIENT -n -q "
    create table tbl_8parts_${layers}granules_rnd (key1 UInt32, sign Int8) engine = CollapsingMergeTree(sign) order by (key1) partition by (key1 % 8);
    insert into tbl_8parts_${layers}granules_rnd select number, 1 from numbers_mt($((layers * 8 * 8192)));
    optimize table tbl_8parts_${layers}granules_rnd final;
    explain pipeline select * from tbl_8parts_${layers}granules_rnd final settings max_threads = 16;" 2>&1 |
       grep -c "CollapsingSortedTransform"
}

for layers in 2 3 5 8; do
  test_random_values $layers
done;

test_sequential_values() {
  layers=$1
  $CLICKHOUSE_CLIENT -n -q "
    create table tbl_8parts_${layers}granules_seq (key1 UInt32, sign Int8) engine = CollapsingMergeTree(sign) order by (key1) partition by (key1 / $((layers * 8192)))::UInt64;
    insert into tbl_8parts_${layers}granules_seq select number, 1 from numbers_mt($((layers * 8 * 8192)));
    optimize table tbl_8parts_${layers}granules_seq final;
    explain pipeline select * from tbl_8parts_${layers}granules_seq final settings max_threads = 8;" 2>&1 |
       grep -c "CollapsingSortedTransform"
}

for layers in 2 3 5 8 16; do
  test_sequential_values $layers
done;
