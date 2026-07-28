#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/107340
# A ClickHouse schema naming a column absent from the Delta column mapping
# (e.g. renamed/dropped in the Delta log) must raise a catchable error, not abort.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DELTA="$CUR_DIR/data_delta_lake/struct_column_mapping"

# Control: the real columns of the Delta table read fine.
$CLICKHOUSE_LOCAL -q "SELECT c0 FROM deltaLakeLocal('$DELTA') ORDER BY c0;"

# Repro: an explicit schema names a column ('cx') that is not in the Delta table's schema. The CREATE is
# now rejected up front with a catchable BAD_ARGUMENTS (the schema does not match the existing table),
# which also prevents the read-time server abort of https://github.com/ClickHouse/ClickHouse/issues/107340.
$CLICKHOUSE_LOCAL --multiquery "
CREATE TABLE t (\`cx\` String) ENGINE = DeltaLakeLocal('$DELTA');
SELECT * FROM t;
" 2>&1 | grep -oF "BAD_ARGUMENTS"
