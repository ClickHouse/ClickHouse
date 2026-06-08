#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Test intersecting ranges"

test_random_values() {
  layers=$1
  $CLICKHOUSE_CLIENT -q "
    drop table if exists tbl_8parts_${layers}granules_rnd;
    create table tbl_8parts_${layers}granules_rnd (key1 UInt32, sign Int8) engine = CollapsingMergeTree(sign) order by (key1) partition by (key1 % 8);
    insert into tbl_8parts_${layers}granules_rnd select number, 1 from numbers_mt($((layers * 8 * 8192)));
    optimize table tbl_8parts_${layers}granules_rnd final;
    explain pipeline select * from tbl_8parts_${layers}granules_rnd final settings max_threads = 16, do_not_merge_across_partitions_select_final = 0;
    drop table tbl_8parts_${layers}granules_rnd;" 2>&1 |
       grep -c "CollapsingSortedTransform"
}

for layers in 2 3 5 8; do
  test_random_values $layers
done;

echo "Test intersecting ranges finished"

echo "Test non intersecting ranges"

test_sequential_values() {
  layers=$1
  $CLICKHOUSE_CLIENT -q "
    drop table if exists tbl_8parts_${layers}granules_seq;
    create table tbl_8parts_${layers}granules_seq (key1 UInt32, sign Int8) engine = CollapsingMergeTree(sign) order by (key1) partition by (key1 / $((layers * 8192)))::UInt64;
    insert into tbl_8parts_${layers}granules_seq select number, 1 from numbers_mt($((layers * 8 * 8192)));
    optimize table tbl_8parts_${layers}granules_seq final;
    explain pipeline select * from tbl_8parts_${layers}granules_seq final settings max_threads = 8, do_not_merge_across_partitions_select_final = 0;
    drop table tbl_8parts_${layers}granules_seq;" 2>&1 |
       grep -c "CollapsingSortedTransform"
}

for layers in 2 3 5 8 16; do
  test_sequential_values $layers
done;

echo "Test non intersecting ranges finished"
