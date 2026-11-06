#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
# no-parallel-replicas: the ProfileEvents with the expected values are reported on the replicas the query runs in,
# and the coordinator does not collect all ProfileEvents values.
# The assertions count PREWHERE read steps and the rows and bytes they read, so the part format and
# the prewhere related settings must be fixed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# RowsReadByPrewhereReaders counts the rows each prewhere read step that reads columns reads, so it
# grows with the number of steps that read: 4 grouped conditions over one Map read 1 x N rows, and
# three steps over three different storage columns read 3 x N.
# gate and gate2 are constant 1, so conditions on them filter nothing and only change how the
# conditions are grouped into steps.
${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t_steps_group;
  DROP TABLE IF EXISTS t_steps_throwing;

  CREATE TABLE t_steps_group (id UInt64, gate UInt8, gate2 UInt8, tags Map(String, String))
  ENGINE = MergeTree ORDER BY id
  SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0,
           ratio_of_defaults_for_sparse_serialization = 1.0;

  CREATE TABLE t_steps_throwing (id UInt64, tags Map(String, String))
  ENGINE = MergeTree ORDER BY id
  SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0,
           ratio_of_defaults_for_sparse_serialization = 1.0;

  INSERT INTO t_steps_group
  SELECT number, 1, 1, mapFromArrays(arrayMap(i -> 'k' || toString(i), range(4)), arrayMap(i -> toString(number + i), range(4)))
  FROM numbers(100000);

  INSERT INTO t_steps_throwing
  SELECT number, map('safe', if(number % 3 = 0, '', 'y'), 'val', if(number % 3 = 0, 'not a number', toString(number % 100)))
  FROM numbers(100000);

  OPTIMIZE TABLE t_steps_group FINAL;
  OPTIMIZE TABLE t_steps_throwing FINAL;
"

query_id_group=group_$CLICKHOUSE_DATABASE
query_id_throwing=throwing_$CLICKHOUSE_DATABASE
query_id_interleaved=interleaved_$CLICKHOUSE_DATABASE
query_id_nested=nested_$CLICKHOUSE_DATABASE
query_id_nested_split=nested_split_$CLICKHOUSE_DATABASE
query_id_read_ahead=read_ahead_$CLICKHOUSE_DATABASE

opts=(
  --enable_analyzer 1
  --optimize_functions_to_subcolumns 1
  --enable_multiple_prewhere_read_steps 1
  --use_query_condition_cache 0
)

# The rows a step reads cannot count a run of steps over one storage column, because only its first
# step reads: MergeTreeRangeReader::continueReadingChain returns before the row counter when a step
# reads no columns, so such a run reads the rows once, exactly like a single step over them would.
# MergeTreeSelectProcessor reports the number of steps at TEST level, so ask it directly.
# Replace the level the runner passes instead of appending: boost multitoken may prefer the first one.
if [ -n "${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:-}" ]; then
  CLICKHOUSE_CLIENT_TEST_LOGS=${CLICKHOUSE_CLIENT//--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=test}
else
  CLICKHOUSE_CLIENT_TEST_LOGS="${CLICKHOUSE_CLIENT} --send_logs_level=test"
fi

# Runs the query and prints the step counts its MergeTreeSelectProcessors reported, deduplicated, so
# that a plan whose processors disagree prints a list and fails the comparison.
read_steps() {
  # shellcheck disable=SC2086
  ${CLICKHOUSE_CLIENT_TEST_LOGS} "${opts[@]}" --query_id "$1" -q "$2" 2>&1 \
    | sed -n 's/.*PREWHERE condition was split into \([0-9]*\) steps.*/\1/p' | sort -u | paste -sd,
}

steps_group=$(read_steps "$query_id_group" "
  SELECT count() FROM t_steps_group
  PREWHERE tags['k0'] != '' AND tags['k1'] != '' AND tags['k2'] != '' AND tags['k3'] != ''
  FORMAT Null
")

steps_throwing=$(read_steps "$query_id_throwing" "
  SELECT count() FROM t_steps_throwing
  PREWHERE tags['safe'] != '' AND toUInt64(tags['val']) > 50
  FORMAT Null
")

# Conditions that may throw get a step each, but the columns of that run of steps are read by its
# first step, so the Map is deserialized once, as for the grouped query above. Every row passes, so
# a step per condition that read its own key would deserialize the whole Map four times.
steps_read_ahead=$(read_steps "$query_id_read_ahead" "
  SELECT count() FROM t_steps_group
  PREWHERE NOT startsWith(tags['k0'], 'z') AND NOT startsWith(tags['k1'], 'z')
       AND NOT startsWith(tags['k2'], 'z') AND NOT startsWith(tags['k3'], 'z')
  FORMAT Null
")

# Only adjacent conditions over the same storage column are merged, so the condition on gate keeps
# the two Map conditions in separate steps and the query reads the rows three times.
steps_interleaved=$(read_steps "$query_id_interleaved" "
  SELECT count() FROM t_steps_group
  PREWHERE tags['k0'] != '' AND gate = 1 AND tags['k1'] != ''
  FORMAT Null
")

# A WHERE moved into an existing PREWHERE arrives as a nested conjunction. Flattening it must not
# fragment conditions that used to be grouped, so these three non throwing conditions over one Map
# still read the rows once.
steps_nested=$(read_steps "$query_id_nested" "
  SELECT count() FROM t_steps_group
  PREWHERE tags['k0'] != ''
  WHERE tags['k1'] != '' AND tags['k2'] != ''
  FORMAT Null
  SETTINGS optimize_prewhere_after_pushdown = 1
")

# The same nested conjunction, but its conditions read different columns, so flattening it splits
# the step it would otherwise be evaluated in: the rows are read three times, not twice.
steps_nested_split=$(read_steps "$query_id_nested_split" "
  SELECT count() FROM t_steps_group
  PREWHERE tags['k0'] != ''
  WHERE gate = 1 AND gate2 = 1
  FORMAT Null
  SETTINGS optimize_prewhere_after_pushdown = 1
")

${CLICKHOUSE_CLIENT} -q "
  SYSTEM FLUSH LOGS query_log;

  -- 4 non throwing conditions over the same Map stay in one step: 100000 rows read once.
  SELECT 'grouped steps', '$steps_group' = '1' AND ProfileEvents['RowsReadByPrewhereReaders'] = 100000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_group' AND type = 'QueryFinish';

  -- The throwing condition gets its own step, which sees only the rows the first step kept. That step
  -- reads no columns, its key was read ahead by the first step, so the Map is still read once.
  SELECT 'split steps', '$steps_throwing' = '2' AND ProfileEvents['RowsReadByPrewhereReaders'] = 100000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_throwing' AND type = 'QueryFinish';

  -- Four steps, one per condition, that read the Map once: at most as many bytes as the grouped query
  -- (the marks may already be cached), far fewer than four reads of the Map.
  SELECT 'read ahead bytes', '$steps_read_ahead' = '4' AND ra.b < 2 * g.b
    FROM (SELECT ProfileEvents['ReadCompressedBytes'] AS b FROM system.query_log
           WHERE current_database = currentDatabase() AND query_id = '$query_id_read_ahead' AND type = 'QueryFinish') AS ra,
         (SELECT ProfileEvents['ReadCompressedBytes'] AS b FROM system.query_log
           WHERE current_database = currentDatabase() AND query_id = '$query_id_group' AND type = 'QueryFinish') AS g;

  -- Exactly three steps over three different storage columns, so each of them reads.
  SELECT 'interleaved steps', '$steps_interleaved' = '3' AND ProfileEvents['RowsReadByPrewhereReaders'] = 300000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_interleaved' AND type = 'QueryFinish';

  -- Flattening a nested conjunction of non throwing conditions over one Map keeps them in one step.
  SELECT 'nested grouped steps', '$steps_nested' = '1' AND ProfileEvents['RowsReadByPrewhereReaders'] = 100000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_nested' AND type = 'QueryFinish';

  -- Without flattening the nested conjunction is one condition and the query reads the rows twice.
  SELECT 'nested split steps', '$steps_nested_split' = '3' AND ProfileEvents['RowsReadByPrewhereReaders'] = 300000
    FROM system.query_log
   WHERE current_database = currentDatabase() AND query_id = '$query_id_nested_split' AND type = 'QueryFinish';

  DROP TABLE t_steps_group;
  DROP TABLE t_steps_throwing;
"
