#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

# `DateTime` must be written as `vortex.timestamp` (not as the generic `U32`),
# so the inferred schema of the produced file stays temporal: `DateTime64(0)`.

echo "Schema inference:"
$CLICKHOUSE_LOCAL -q "
    SELECT
        toDateTime('2020-01-01 01:02:03', 'UTC') + number AS dt,
        if(number % 2 = 0, NULL, toDateTime('2020-01-01 01:02:03', 'Asia/Istanbul') + number) AS dt_nullable,
        [toDateTime('2020-01-01 01:02:03', 'UTC') + number] AS dt_arr
    FROM numbers(3)
    FORMAT Vortex" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "DESC file('$DATA_FILE', 'Vortex')"

echo "Round trip:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex') FORMAT TSV"

echo "Round trip with an explicit DateTime schema:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex', \"dt DateTime('UTC'), dt_nullable Nullable(DateTime('Asia/Istanbul'))\") FORMAT TSV"

rm -f "$DATA_FILE"
