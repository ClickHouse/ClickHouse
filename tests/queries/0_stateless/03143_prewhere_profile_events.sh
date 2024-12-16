#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
  DROP TABLE IF EXISTS t;

  CREATE TABLE t(a UInt32, b UInt32, c UInt32, d UInt32) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

  INSERT INTO t SELECT number, number, number, number FROM numbers_mt(1e7);

  OPTIMIZE TABLE t FINAL;
"

query_id_1=$RANDOM$RANDOM
query_id_2=$RANDOM$RANDOM
query_id_3=$RANDOM$RANDOM
query_id_4=$RANDOM$RANDOM

client_opts=(
  --max_block_size 65409
  --max_threads    8
)

${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query_id "$query_id_1" -q "
  SELECT *
    FROM t
PREWHERE (b % 8192) = 42
   WHERE c = 42
  FORMAT Null
"

${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query_id "$query_id_2" -q "
  SELECT *
    FROM t
PREWHERE (b % 8192) = 42 AND (c % 8192) = 42
   WHERE d = 42
  FORMAT Null
settings enable_multiple_prewhere_read_steps=1;
"

${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query_id "$query_id_3" -q "
  SELECT *
    FROM t
PREWHERE (b % 8192) = 42 AND (c % 16384) = 42
   WHERE d = 42
  FORMAT Null
settings enable_multiple_prewhere_read_steps=0;
"

${CLICKHOUSE_CLIENT} "${client_opts[@]}" --query_id "$query_id_4" -q "
  SELECT b, c
    FROM t
PREWHERE (b % 8192) = 42 AND (c % 8192) = 42
  FORMAT Null
settings enable_multiple_prewhere_read_steps=1;
"

${CLICKHOUSE_CLIENT} -q "
  SYSTEM FLUSH LOGS;

  -- 52503 which is 43 * number of granules, 10000000
  SELECT ProfileEvents['RowsReadByMainReader'], ProfileEvents['RowsReadByPrewhereReaders']
    FROM system.query_log
   WHERE current_database=currentDatabase() AND query_id = '$query_id_1' and type = 'QueryFinish';

  -- 52503, 10052503 which is the sum of 10000000 from the first prewhere step plus 52503 from the second
  SELECT ProfileEvents['RowsReadByMainReader'], ProfileEvents['RowsReadByPrewhereReaders']
    FROM system.query_log
   WHERE current_database=currentDatabase() AND query_id = '$query_id_2' and type = 'QueryFinish';

  -- 26273 the same as query #1 but twice less data (43 * ceil((52503 / 43) / 2)), 10000000
  SELECT ProfileEvents['RowsReadByMainReader'], ProfileEvents['RowsReadByPrewhereReaders']
    FROM system.query_log
   WHERE current_database=currentDatabase() AND query_id = '$query_id_3' and type = 'QueryFinish';

  -- 0, 10052503
  SELECT ProfileEvents['RowsReadByMainReader'], ProfileEvents['RowsReadByPrewhereReaders']
    FROM system.query_log
   WHERE current_database=currentDatabase() AND query_id = '$query_id_4' and type = 'QueryFinish';
"
