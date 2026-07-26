#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings
# The assertions count PREWHERE read steps through RowsReadByPrewhereReaders, so the part format and
# the prewhere related settings must be fixed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# RowsReadByPrewhereReaders counts the rows each prewhere read step reads, so it grows with the
# number of steps: 4 grouped conditions over one Map read 1 x N rows, and a split that puts the
# throwing condition into its own step reads 2 x N.
${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_steps_group;
  DROP TABLE IF EXISTS t_steps_throwing;

  CREATE TABLE t_steps_group (id UInt64, tags Map(String, String))
  ENGINE = MergeTree ORDER BY id
  SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

  CREATE TABLE t_steps_throwing (id UInt64, tags Map(String, String))
  ENGINE = MergeTree ORDER BY id
  SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

  INSERT INTO t_steps_group
  SELECT number, mapFromArrays(arrayMap(i -> 'k' || toString(i), range(4)), arrayMap(i -> toString(number + i), range(4)))
  FROM numbers(100000);

  INSERT INTO t_steps_throwing
  SELECT number, map('safe', if(number % 3 = 0, '', 'y'), 'val', if(number % 3 = 0, 'not a number', toString(number % 100)))
  FROM numbers(100000);

  OPTIMIZE TABLE t_steps_group FINAL;
  OPTIMIZE TABLE t_steps_throwing FINAL;
"

query_id_group=group_$CLICKHOUSE_DATABASE
query_id_throwing=throwing_$CLICKHOUSE_DATABASE
query_id_nested=nested_$CLICKHOUSE_DATABASE

opts=(
  --optimize_functions_to_subcolumns 1
  --enable_multiple_prewhere_read_steps 1
  --use_query_condition_cache 0
)

${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id "$query_id_group" -q "
  SELECT count() FROM t_steps_group
  PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '' AND tags['k3'] != ''
  FORMAT Null
"

${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id "$query_id_throwing" -q "
  SELECT count() FROM t_steps_throwing
  PREWHERE tags['safe'] != '' AND toUInt64(tags['val']) > 50
  FORMAT Null
"

# A WHERE moved into an existing PREWHERE arrives as a nested conjunction. Flattening it must not
# fragment conditions that used to be grouped, so these three non throwing conditions over one Map
# still read the rows once.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id "$query_id_nested" -q "
  SELECT count() FROM t_steps_group
  PREWHERE tags['k0'] != ''
  WHERE tags['k1'] != '' AND tags['k2'] != ''
  FORMAT Null
  SETTINGS optimize_prewhere_after_pushdown = 1
"

${CLICKHOUSE_CLIENT} -q "
  SYSTEM FLUSH LOGS query_log;

  -- 4 non throwing conditions over the same Map stay in one step: 100000 rows read once.
  SELECT 'grouped steps', ProfileEvents['RowsReadByPrewhereReaders'] = 100000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_group' AND type = 'QueryFinish';

  -- The throwing condition gets its own step, so the rows are read by two prewhere readers. The
  -- second reader sees fewer rows because the first step filters them.
  SELECT 'split steps', ProfileEvents['RowsReadByPrewhereReaders'] > 100000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_throwing' AND type = 'QueryFinish';

  -- Flattening a nested conjunction of non throwing conditions keeps them in one step.
  SELECT 'nested grouped steps', ProfileEvents['RowsReadByPrewhereReaders'] = 100000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_nested' AND type = 'QueryFinish';

  DROP TABLE t_steps_group;
  DROP TABLE t_steps_throwing;
"
