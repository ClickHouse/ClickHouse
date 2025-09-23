#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings
# - no-random-merge-tree-settings -- may change amount of granulas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
  drop table if exists data;
  create table data (key Int, value Int) engine=MergeTree() order by key;
  insert into data select *, *+1000000 from numbers(100000);
"

table_uuid="$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND table = 'data'")"

$CLICKHOUSE_CLIENT -nm -q "
  -- { echo }
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid');
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '');
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', key = 8193);
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', key >= 8193);
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', key = 8192+1 or key = 8192*3+1);
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', key = 8192+1 or key = 8192*5+1);

  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', 'all_1_1_0', key = 8193);
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', 'no_such_part', key = 8193);

  -- Columns not from PK is allowed and ignored.
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', value = 0);
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', key = 8193 and value = 0);

  -- Set
  select * from mergeTreeAnalyzeIndexUUID('$table_uuid', '', key in (8193, 16385));
" |& sed "s/$table_uuid/UUID/"
