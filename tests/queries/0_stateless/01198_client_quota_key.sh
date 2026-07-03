#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --quota_key Hello --query_id test_quota_key --log_queries 1 --query "SELECT 1; SYSTEM FLUSH LOGS; SELECT DISTINCT quota_key FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - 300 AND query_id = 'test_quota_key'"
