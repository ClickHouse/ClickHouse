#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The persisted default database of `clickhouse-local` stays readable when another database is the default.

WORKING_FOLDER="${CLICKHOUSE_TMP}/05293_clickhouse_local_default_database_metadata_file"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

echo "--- custom default database, then the canonical one ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/mydb" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (42)" \
    -- --default_database=mydb
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/mydb" -q "
    SELECT x FROM mydb.t;
    SELECT uuid = '$(basename "$(readlink "${WORKING_FOLDER}/mydb/metadata/mydb")")' FROM system.databases WHERE name = 'mydb'"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/mydb" -q "SELECT x FROM t" -- --default_database=mydb

echo "--- canonical default database, then a custom one ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/default" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (7)"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/default" -q "SELECT x FROM default.t" -- --default_database=other
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/default" -q "SELECT x FROM t"

echo "--- a directory without the metadata file gets it on the next run ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/old" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (5)"
rm "${WORKING_FOLDER}/old/metadata/default.sql"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/old" -q "SELECT x FROM t"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/old" -q "SELECT x FROM default.t" -- --default_database=other

echo "--- a table created after a stateless run uses the UUID from the metadata file ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/stateless" -q "SELECT 1"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/stateless" \
    -q "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x; INSERT INTO t VALUES (3)"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/stateless" -q "SELECT x FROM default.t" -- --default_database=other

echo "--- --only-system-tables writes no metadata ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/system" --only-system-tables -q "SELECT 1"
[ -e "${WORKING_FOLDER}/system/metadata" ] && echo "metadata exists" || echo "no metadata"

rm -rf "${WORKING_FOLDER}"
