#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
# `DeltaLakeLocal` recognises the data lake settings, as the other `DeltaLake` engines do, so a name that is both a data
# lake setting and a query setting stays with the table instead of being applied to the `CREATE` query and dropped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_delta"
rm -rf "$TABLE_PATH"
mkdir -p "$TABLE_PATH"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_delta_local (a Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH}')
SETTINGS iceberg_compaction_commit_batch_size = 7, allow_dynamic_metadata_for_data_lakes = 1"

# The path is unique to the run, so only the SETTINGS clause is printed.
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE t_delta_local FORMAT TSVRaw" | grep -o 'SETTINGS .*'

$CLICKHOUSE_CLIENT -q "DROP TABLE t_delta_local"
rm -rf "$TABLE_PATH"
