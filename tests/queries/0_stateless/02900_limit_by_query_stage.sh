#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --stage with_mergeable_state --query 'SELECT dummy FROM system.one WHERE (dummy + dummy) >= 0 LIMIT 1 BY (dummy + dummy) + 0 AS l'
$CLICKHOUSE_CLIENT --stage with_mergeable_state_after_aggregation --query 'SELECT dummy FROM system.one WHERE (dummy + dummy) >= 0 LIMIT 1 BY (dummy + dummy) + 0 AS l'
$CLICKHOUSE_CLIENT --stage with_mergeable_state_after_aggregation_and_limit --query 'SELECT dummy FROM system.one WHERE (dummy + dummy) >= 0 LIMIT 1 BY (dummy + dummy) + 0 AS l'
