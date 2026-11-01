#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for #101831: `clickhouse-local --path X -- --default_database=mydb`
# silently lost tables on restart.
#
# `createClickHouseLocalDatabaseOverlay` looked up the persisted database UUID
# at `metadata/default`, but `DatabaseAtomic` creates the symlink at
# `metadata/<escapeForFileName(name)>`. When the configured default database was
# anything other than `default`, the lookup never matched, a fresh UUID was
# minted, the previous run's data was orphaned, and the table appeared as
# `UNKNOWN_TABLE` on the next invocation.

WORKING_FOLDER="${CLICKHOUSE_TMP}/04252_clickhouse_local_default_database_persistence_101831"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

echo "--- custom default_database (mydb): first run creates and selects the table ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/mydb" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (42); SELECT x FROM t" \
    -- --default_database=mydb

echo "--- custom default_database (mydb): second run must see the persisted table ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/mydb" \
    -q "SELECT x FROM t" \
    -- --default_database=mydb

echo "--- canonical 'default' default database: no regression ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/default" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (7); SELECT x FROM t"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/default" \
    -q "SELECT x FROM t"

rm -rf "${WORKING_FOLDER}"
