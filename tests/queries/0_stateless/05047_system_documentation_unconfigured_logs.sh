#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# System log tables are documented even when they are not configured and therefore do not exist in `system.tables`.
$CLICKHOUSE_LOCAL --query "
    SELECT
        name,
        name IN
        (
            SELECT name
            FROM system.tables
            WHERE database = 'system'
        ) AS attached,
        source,
        notEmpty(description),
        position(description, '**Columns**') > 0
    FROM system.documentation
    WHERE type = 'System Table'
        AND name IN ('error_log', 'query_log', 'trace_log')
    ORDER BY name"
