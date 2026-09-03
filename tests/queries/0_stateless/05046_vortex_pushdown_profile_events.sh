#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `ProfileEvents` of the filter pushdown prove what actually reached the scan, which result
# equivalence alone cannot show, since ClickHouse reapplies WHERE either way.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
DATA_FILE="${WORKING_DIR}/data.vortex"

# Several splits (they hold at most 100 000 rows each), so that a selective filter provably drops
# whole splits.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', 'Vortex')
    SELECT number AS n, toString(number) AS s, toDate32('2020-01-01') + number % 1000 AS d
    FROM numbers(300000)
    SETTINGS engine_file_truncate_on_insert = 1"

run_and_report_events() {
    local label=$1
    local query=$2
    local query_id="${CLICKHOUSE_DATABASE}_vortex_pushdown_$RANDOM$RANDOM"
    echo "$label"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "$query"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT
            ProfileEvents['VortexFilterPushdownConjunctsPushed'],
            ProfileEvents['VortexFilterPushdownConjunctsDropped'],
            ProfileEvents['VortexScanSplits'] >= 3,
            ProfileEvents['VortexScanEmptySplits'] >= 2
        FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
}

run_and_report_events "A fully translated conjunction (both conjuncts pushed, other splits dropped whole):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n = 42 AND d = '2020-02-12'"

run_and_report_events "A partly translatable condition (one conjunct pushed, one left to ClickHouse):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n = 42 AND n % 2 = 0"

run_and_report_events "An untranslatable condition (everything left to ClickHouse, no split dropped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n % 100000 = 17"

run_and_report_events "Pushdown disabled:" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n = 42 SETTINGS input_format_vortex_filter_push_down = 0"

rm -rf "${WORKING_DIR}"
