#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The ORC schema reader must expose the number of rows from ORC metadata during schema
# inference (readNumberOrRows). This seeds system.schema_inference_cache.number_of_rows,
# which enables the count-from-cache fast path for file()/url()/object storage sources.

DATA_FILE="$CLICKHOUSE_TMP/04512_orc_row_count.orc"

$CLICKHOUSE_LOCAL -q "SELECT number AS x FROM numbers(100) FORMAT ORC" > "$DATA_FILE"

$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', ORC) SETTINGS optimize_count_from_files = 0"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_FILE', ORC) SETTINGS optimize_count_from_files = 1"

$CLICKHOUSE_LOCAL -m -q "
DESC file('$DATA_FILE', ORC) FORMAT Null;
SELECT number_of_rows FROM system.schema_inference_cache WHERE format = 'ORC';
"

rm "$DATA_FILE"
