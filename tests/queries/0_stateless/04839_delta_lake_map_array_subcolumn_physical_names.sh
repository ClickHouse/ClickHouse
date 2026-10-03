#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# Reading a subcolumn of a `Map` or `Array(Tuple(...))` column from a Delta Lake table with
# `delta.columnMapping.mode = 'name'` used to fail with
# `Not found column data.keys in physical names map` (LOGICAL_ERROR), because the whole dotted
# subcolumn name was looked up in the Delta Lake column mapping, which is keyed by Delta field
# paths: a `Map` has no `keys` field, and array elements live under an extra `array_element` hop.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="deltaLakeLocal('$CUR_DIR/data_delta_lake/map_array_column_mapping')"

$CLICKHOUSE_LOCAL -q "
SELECT id, data, deductions FROM $TABLE ORDER BY id;
SELECT data.keys FROM $TABLE ORDER BY 1;
SELECT mapKeys(data) FROM $TABLE ORDER BY 1;
SELECT data.values FROM $TABLE ORDER BY 1;
SELECT deductions.transaction_type FROM $TABLE ORDER BY 1;
SELECT deductions.amount FROM $TABLE ORDER BY 1;
SELECT data.size0, deductions.size0 FROM $TABLE ORDER BY 1;
"
