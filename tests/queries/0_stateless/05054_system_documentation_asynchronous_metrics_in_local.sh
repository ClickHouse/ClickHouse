#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.asynchronous_metrics` is attached only by the server, but its documentation is owned by the source,
# so the page must also be available in `clickhouse-local`, which is how `utils/generate-system-tables-docs` runs.
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'asynchronous_metrics'"

${CLICKHOUSE_LOCAL} --query "
    SELECT
        source,
        description LIKE '%## Description {#description}%' AS has_description,
        description LIKE '%## Columns {#columns}%' AS has_columns,
        description LIKE '%## Metric descriptions {#metric-descriptions}%' AS has_metric_descriptions,
        description LIKE '%### AsynchronousMetricsUpdateInterval {#asynchronousmetricsupdateinterval}%' AS has_known_metric,
        position(description, '### AsynchronousHeavyMetricsCalculationTimeSpent')
            < position(description, '### AsyncLoggingQueueSize') AS has_case_insensitive_metric_order,
        description LIKE '%## Examples {#examples}%' AS has_examples,
        description LIKE '%{{ASYNCHRONOUS_METRICS}}%' AS has_unresolved_placeholder
    FROM system.documentation
    WHERE type = 'System Table' AND name = 'asynchronous_metrics'"

# On the server, the page must still use the static source catalog. Runtime metric names vary with settings and
# include concrete resource names, while this documented wildcard name is present only in the source catalog.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count(),
        countIf(description LIKE '%### HTTPConnectionPool*group_name*TCPRcvBufTotalBytes {#httpconnectionpoolgroup_nametcprcvbuftotalbytes}%'),
        countIf(description LIKE '%{{ASYNCHRONOUS_METRICS}}%')
    FROM system.documentation
    WHERE type = 'System Table' AND name = 'asynchronous_metrics'"
