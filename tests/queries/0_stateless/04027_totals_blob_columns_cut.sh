#!/usr/bin/env bash
# Tags: distributed

# Regression test for ColumnBLOB assertion failure in prepareTotals (issue #98700).
# When a distributed query with WITH TOTALS and arrayJoin produces multi-row totals,
# prepareTotals calls cut on ColumnBLOB columns. Without the fix, this triggers
# an assertion failure in debug/sanitizer builds because ColumnBLOB::cloneEmpty
# returns the wrapped column type.
# The fix: ColumnBLOB overrides cut to delegate to the wrapped/deserialized column.
# We use head -1 because totals propagation through remote() depends on settings.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    SELECT DISTINCT x
    FROM remote('127.0.0.{1,2}', view(
        SELECT 1 AS x GROUP BY 'a' WITH TOTALS
    ))
    WHERE arrayJoin([1, 1]) = x
    SETTINGS parallel_replicas_for_cluster_engines = 0
" 2>&1 | head -1
