#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="03276_null_format_matching_case_insensitive_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "select * from numbers_mt(1e8) format null settings max_rows_to_read=0"

$CLICKHOUSE_CLIENT -q "
  SYSTEM FLUSH LOGS query_log;

  -- SendBytes should be close to 0, previously for this query it was around 800MB
  select ProfileEvents['NetworkSendBytes'] < 1e6 from system.query_log where current_database = currentDatabase() and event_date >= yesterday() and query_id = '$query_id' and type = 'QueryFinish';
"
