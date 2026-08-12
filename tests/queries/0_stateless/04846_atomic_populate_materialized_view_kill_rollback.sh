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
    CREATE TABLE src_04846 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO src_04846 SELECT number FROM numbers(10);
"

QUERY_ID="populate_04846_${CLICKHOUSE_DATABASE}"

# `sleepEachRow` keeps the population running while it is killed.
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "
    CREATE MATERIALIZED VIEW mv_04846 ENGINE = MergeTree ORDER BY id POPULATE
        AS SELECT id, sleepEachRow(0.2) AS s FROM src_04846
" > /dev/null 2>&1 &
CREATE_PID=$!

# The source is subscribed to the view at the cut, before the population starts reading.
for _ in {1..300}; do
    [[ "$($CLICKHOUSE_CLIENT -q "SELECT has(dependencies_table, 'mv_04846') FROM system.tables WHERE database = currentDatabase() AND name = 'src_04846'")" == "1" ]] && break
    sleep 0.05
done

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"

wait $CREATE_PID
CREATE_STATUS=$?

echo "create failed: $((CREATE_STATUS != 0))"
$CLICKHOUSE_CLIENT -q "
    SELECT 'view left behind:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'mv_04846';
    SELECT 'source has dependents:', count() FROM system.tables WHERE database = currentDatabase() AND name = 'src_04846' AND notEmpty(dependencies_table);
    DROP TABLE src_04846;
"
