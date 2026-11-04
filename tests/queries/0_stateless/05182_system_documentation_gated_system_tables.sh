#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.zookeeper*`, `system.keeper_*` and `system.transactions` are attached only where ZooKeeper, an in-process
# Keeper or experimental transactions are configured, but their documentation is owned by the source, so their pages
# must also be available in `clickhouse-local`, which is how `utils/generate-system-tables-docs` runs.
${CLICKHOUSE_LOCAL} --query "
    SELECT
        name,
        source,
        description LIKE '%## Description {#description}%' AS has_description,
        description LIKE '%## Columns {#columns}%' AS has_columns
    FROM system.documentation
    WHERE type = 'System Table'
        AND name IN ('keeper_cluster', 'transactions', 'zookeeper', 'zookeeper_info', 'zookeeper_watches')
    ORDER BY name"

# The documentation of an unavailable table does not make it queryable.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'zookeeper_watches'"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.zookeeper_watches" 2>&1 | grep -c "UNKNOWN_TABLE"

# Every documented table is still emitted exactly once, whether it is attached in this process or not.
${CLICKHOUSE_LOCAL} --query "
    SELECT count()
    FROM
    (
        SELECT name
        FROM system.documentation
        WHERE type = 'System Table'
        GROUP BY name
        HAVING count() > 1
    )"
