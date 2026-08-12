#!/usr/bin/env bash
# Tags: no-replicated-database
# - no-replicated-database - there the CREATE is an entry of the replicated DDL log, which uses the legacy
#   population and does not roll the view back.

# Killing a running `CREATE MATERIALIZED VIEW ... POPULATE` must cancel the population and leave nothing
# behind: no view, no subscription of the source.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_04846;
    DROP TABLE IF EXISTS src_04846;
    CREATE TABLE src_04846 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO src_04846 SELECT number FROM numbers(100);
"

QUERY_ID="populate_04846_${CLICKHOUSE_DATABASE}"
CREATE_ERR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

# 30 seconds of `sleepEachRow` keep the population running until it is killed, whatever the load is.
$CLICKHOUSE_CLIENT --function_sleep_max_microseconds_per_block 60000000 --query_id "$QUERY_ID" -q "
    CREATE MATERIALIZED VIEW mv_04846 ENGINE = MergeTree ORDER BY id POPULATE
        AS SELECT id, sleepEachRow(0.3) AS s FROM src_04846
" > /dev/null 2> "$CREATE_ERR" &
CREATE_PID=$!

# The source is subscribed to the view at the cut, before the population starts reading.
SUBSCRIBED=0
DEADLINE=$((SECONDS + 15))
while (( SECONDS < DEADLINE )); do
    if [[ "$($CLICKHOUSE_CLIENT -q "SELECT has(dependencies_table, 'mv_04846') FROM system.tables WHERE database = currentDatabase() AND name = 'src_04846'")" == "1" ]]; then
        SUBSCRIBED=1
        break
    fi
    sleep 0.05
done
echo "subscribed before kill: $SUBSCRIBED"

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"

wait $CREATE_PID

echo "create cancelled: $(grep -c QUERY_WAS_CANCELLED "$CREATE_ERR")"
rm -f "$CREATE_ERR"

$CLICKHOUSE_CLIENT -q "
    SELECT 'view left behind:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04846';
    SELECT 'source has dependents:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'src_04846' AND notEmpty(dependencies_table);
    DROP TABLE IF EXISTS mv_04846;
    DROP TABLE src_04846;
"
