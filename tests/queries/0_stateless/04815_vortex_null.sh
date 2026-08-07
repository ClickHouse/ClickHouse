#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

# The ClickHouse `Nothing` type (e.g. `SELECT NULL`) is written as the Vortex `Null` type,
# and a Vortex `Null` field is schema-inferred back as `Nullable(Nothing)`.

echo "Schema inference:"
$CLICKHOUSE_LOCAL -q "SELECT NULL AS x, [] AS arr, materialize(NULL) AS y FROM numbers(3) FORMAT Vortex" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "DESC file('$DATA_FILE', 'Vortex')"

echo "Round trip:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex') FORMAT TSV"

echo "Count:"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', 'Vortex')"

rm -f "$DATA_FILE"
