#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A real `Bool` column is a `UInt8` with a custom name, and it is written as an Arrow `BOOL` file
# column - a pairing the pushdown has to accept. Result equivalence alone cannot show it, because
# ClickHouse reapplies the `WHERE` either way, so this asserts the `ProfileEvents` of the scan.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')
WORKING_DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
DATA_FILE="${WORKING_DIR}/data.vortex"

# `b` is true in exactly one of the splits (they hold at most 100 000 rows each), so a pushed
# predicate on it provably drops whole splits.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', 'Vortex')
    SELECT number AS n, (intDiv(number, 100000) = 1)::Bool AS b
    FROM numbers(300000)
    SETTINGS engine_file_truncate_on_insert = 1"

echo "The column reads back as Bool:"
$CLICKHOUSE_CLIENT -q "DESCRIBE file('$DATA_FILE', 'Vortex')" | cut -f1,2

run_and_report_events() {
    local label=$1
    local query=$2
    local query_id="${CLICKHOUSE_DATABASE}_vortex_pushdown_bool_$RANDOM$RANDOM"
    echo "$label"
    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "$query"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT
            ProfileEvents['VortexFilterPushdownConjunctsPushed'],
            ProfileEvents['VortexFilterPushdownConjunctsDropped'],
            ProfileEvents['VortexScanEmptySplits'] >= 1
        FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
}

run_and_report_events "b = true (pushed, whole splits are dropped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE b = true"

run_and_report_events "bare b (pushed, whole splits are dropped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE b"

run_and_report_events "NOT b (pushed, whole splits are dropped):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE NOT b"

rm -rf "${WORKING_DIR}"
