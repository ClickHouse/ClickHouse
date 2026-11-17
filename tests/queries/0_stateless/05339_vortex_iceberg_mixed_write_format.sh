#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds, and `IcebergLocal` needs
# the USE_AVRO build option

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The Vortex data files of an Iceberg table that is now written in another format (`ORC`): the
# format of the table is only the one new files are written in, so the Vortex files of the
# snapshot must still get filter pushdown under the names of their own schema after a
# `RENAME COLUMN`, and lazy materialization when every data file is eligible.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05339_vortex_iceberg_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# `clickhouse-local` takes its working directory for the user files directory.
cd "${LOCAL_DIR}" || exit 1

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg 1 --query "
    CREATE TABLE t (a Int64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'Vortex');
    INSERT INTO t SELECT number, toString(number) FROM numbers(1000);
    ALTER TABLE t RENAME COLUMN a TO b;
    ALTER TABLE t ADD COLUMN a Nullable(Int64);
    INSERT INTO t SELECT number, toString(number), number FROM numbers(1000, 10);

    CREATE TABLE l (k Int64, s String) ENGINE = IcebergLocal('${LOCAL_DIR}/l/', 'Vortex');
    INSERT INTO l SELECT number, concat('val_', toString(number)) FROM numbers(5000);
"

TABLE_DEFINITION="
    CREATE TABLE t ENGINE = IcebergLocal('${LOCAL_DIR}/t/', 'ORC');
    CREATE TABLE l ENGINE = IcebergLocal('${LOCAL_DIR}/l/', 'ORC');"

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

echo '-- the renamed column is pushed down into both data files'
run_counting_pushdown "SELECT count() FROM t WHERE b < 100"
run "SELECT count() FROM t WHERE b < 100 SETTINGS input_format_vortex_filter_push_down = 0"

echo '-- the new column is pushed down only into the data file that has it'
run_counting_pushdown "SELECT count(), any(s) FROM t WHERE a = 1005"
run "SELECT count(), any(s) FROM t WHERE a = 1005 SETTINGS input_format_vortex_filter_push_down = 0"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer, and some CI
# configurations run with the old analyzer.
echo '-- lazy materialization of the Vortex data files'
for enabled in 1 0; do
    run "
        SET enable_analyzer = 1, query_plan_optimize_lazy_materialization = 1,
            query_plan_max_limit_for_lazy_materialization = 0,
            query_plan_optimize_lazy_materialization_for_object_storage = $enabled;
        SELECT k, s FROM l ORDER BY k DESC LIMIT 3;
        SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM l ORDER BY k LIMIT 3);"
done
