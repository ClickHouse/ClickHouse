#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds, and `IcebergLocal` needs
# the USE_AVRO build option

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Filter pushdown into the Vortex data files of an Iceberg table after a `RENAME COLUMN`. The data
# file written before the rename uses the old column names, and the filter uses the current ones:
# a filter on the renamed column has to reach that file under its old name, and a filter on a new
# column that took the old name must not reach the old column of that file.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05332_vortex_iceberg_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# `clickhouse-local` takes its working directory for the user files directory.
cd "${LOCAL_DIR}" || exit 1

TABLE_DEFINITION="CREATE TABLE t ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Vortex');"

# The counters of `system.events` are those of one `clickhouse-local` process, so each check gets
# a process of its own and they count the scans of that one query.
run()
{
    ${CLICKHOUSE_LOCAL} --query "${TABLE_DEFINITION} $1"
}

run_counting_pushdown()
{
    run "
        $1 SETTINGS use_iceberg_partition_pruning = 0;
        SELECT
            sumIf(value, event = 'VortexFilterPushdownConjunctsPushed') AS pushed,
            sumIf(value, event = 'VortexFilterPushdownConjunctsDropped') AS dropped
        FROM system.events
        WHERE event LIKE 'VortexFilterPushdownConjuncts%';"
}

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE t (a UInt64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Vortex');
    INSERT INTO t SELECT number, toString(number) FROM numbers(1000);
    ALTER TABLE t RENAME COLUMN a TO b;
    ALTER TABLE t ADD COLUMN a Nullable(UInt64);
    INSERT INTO t SELECT number, toString(number), number FROM numbers(1000, 10);
"

echo '-- the renamed column is pushed down into both data files'
run_counting_pushdown "SELECT count() FROM t WHERE b < 100"
run "SELECT count() FROM t WHERE b < 100 SETTINGS input_format_vortex_filter_push_down = 0"

echo '-- the new column is pushed down only into the data file that has it'
run_counting_pushdown "SELECT count(), any(s) FROM t WHERE a = 1005"
run "SELECT count(), any(s) FROM t WHERE a = 1005 SETTINGS input_format_vortex_filter_push_down = 0"

echo '-- a condition on both columns'
run_counting_pushdown "SELECT count() FROM t WHERE b >= 995 AND a > 1000"
run "SELECT count() FROM t WHERE b >= 995 AND a > 1000 SETTINGS input_format_vortex_filter_push_down = 0"

echo '-- both data files read under the current names'
run "SELECT b, s, a FROM t WHERE b IN (5, 1005) ORDER BY b"
