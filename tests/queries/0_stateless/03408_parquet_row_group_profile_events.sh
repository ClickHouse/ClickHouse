#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


query_id="${CLICKHOUSE_DATABASE}_parquet_group_group_prune_profile_$RANDOM"

# A parquet file with 10 row groups from 1 to 100, 10 rows per row group.
${CLICKHOUSE_CLIENT} "${opts[@]}" --query_id="${query_id}" --query="SELECT * FROM file('$CUR_DIR/data_parquet/row_group.parquet') WHERE col_int > 70 format Null" --format Null

${CLICKHOUSE_CLIENT} -nq "
  SYSTEM FLUSH LOGS query_log;

  SELECT ProfileEvents['ParquetReadRowGroups'], ProfileEvents['ParquetPrunedRowGroups']
    FROM system.query_log
	WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' and current_database = currentDatabase();
"

