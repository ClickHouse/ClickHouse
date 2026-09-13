#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.
#
# When a `CREATE TABLE` has a single `SETTINGS` clause, every setting the engine does not recognise but
# the query context does is moved to the query context instead of the table. `DeltaLakeLocal` was
# registered with the predicate of the plain object storage engines rather than of the data lake
# settings it actually uses, so a data lake setting that shares its name with a query setting -
# `iceberg_max_number_datafiles_to_compact` here, one of eleven - silently left the table.
#
# The table is never read, so its path does not have to hold a Delta table: `DeltaLakeLocal` opens it
# lazily.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE dll_tbl (x Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH}')
SETTINGS iceberg_max_number_datafiles_to_compact = 7"

echo "-- the table keeps the setting"
$CLICKHOUSE_CLIENT -q "SHOW CREATE TABLE dll_tbl" | grep -oE "iceberg_max_number_datafiles_to_compact = [0-9]+"

$CLICKHOUSE_CLIENT -q "
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'dll_tbl' AND name = 'iceberg_max_number_datafiles_to_compact'"

$CLICKHOUSE_CLIENT -q "DROP TABLE dll_tbl"
