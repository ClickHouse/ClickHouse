#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `VortexExpressionConverter::convertIn` never executes an `IN` subquery itself - it only inspects a
# set that is already built - so the reading step has to materialize the set before the scan starts.
# `file()` and `url()` must do that for `Vortex` just like the object storage step does, otherwise
# the `IN` pushdown is silently a no-op on those entrypoints while it works on the other one.
# Result equivalence cannot show this, because ClickHouse reapplies the `WHERE` either way; the
# `ProfileEvents` of the pushdown are what proves the set reached the scan.

USER_FILES_PATH=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
WORKING_DIR="${USER_FILES_PATH%/}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${WORKING_DIR}"
DATA_FILE="${WORKING_DIR}/data.vortex"

# Several splits (they hold at most 100 000 rows each), so that a selective filter provably drops
# whole splits.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('$DATA_FILE', 'Vortex')
    SELECT number AS n FROM numbers(300000)
    SETTINGS engine_file_truncate_on_insert = 1"

# A real table, so that the `IN` operand stays a subquery set the reading step has to build, rather
# than a constant the analyzer folds away.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE keys (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO keys SELECT number FROM numbers(3);"

run_and_report_events() {
    local label=$1
    local query=$2
    local query_id="${CLICKHOUSE_DATABASE}_vortex_in_pushdown_$RANDOM$RANDOM"
    echo "$label"
    $CLICKHOUSE_CLIENT --input_format_vortex_preserve_order 1 --query_id="$query_id" -q "$query"
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT
            ProfileEvents['VortexFilterPushdownConjunctsPushed'],
            ProfileEvents['VortexFilterPushdownConjunctsDropped'],
            ProfileEvents['VortexScanEmptySplits'] >= 2
        FROM system.query_log
        WHERE event_date >= yesterday() AND query_id = '$query_id' AND type = 'QueryFinish' AND current_database = currentDatabase()"
}

run_and_report_events "An IN over a subquery set, pushed down (the other splits are dropped whole):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n IN (SELECT k FROM keys)"

run_and_report_events "A NOT IN over the same subquery set, pushed down (it drops no whole split):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n NOT IN (SELECT k FROM keys)"

run_and_report_events "An IN over a literal tuple, for comparison (its set needs no eager pass):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n IN (0, 1, 2)"

run_and_report_events "A subquery set above the 64-element pushdown limit (left to ClickHouse):" \
    "SELECT count() FROM file('$DATA_FILE', 'Vortex') WHERE n IN (SELECT number FROM numbers(100))"

rm -rf "${WORKING_DIR}"
