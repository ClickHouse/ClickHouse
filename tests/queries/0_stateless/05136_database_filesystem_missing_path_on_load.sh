#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `Filesystem` database is rebuilt from its stored `ATTACH DATABASE` statement on every start, and metadata
# loading stops at the first exception. A backing directory that has been removed in the meantime must
# therefore leave the database attached without tables, not make the server refuse to start.

WORKING_FOLDER="${CLICKHOUSE_TMP}/05136_database_filesystem_missing_path_on_load"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/files"

echo "--- first run: create the database over an existing directory ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store" \
    -q "CREATE DATABASE fsdb ENGINE = Filesystem('${WORKING_FOLDER}/files');
        SELECT name FROM system.databases WHERE name = 'fsdb'"

rm -rf "${WORKING_FOLDER}/files"

echo "--- second run: the directory is gone; the database still loads, serves no tables and drops ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store" \
    -q "SELECT name FROM system.databases WHERE name = 'fsdb';
        SELECT count() FROM system.tables WHERE database = 'fsdb';
        DROP DATABASE fsdb;
        SELECT count() FROM system.databases WHERE name = 'fsdb'"

rm -rf "${WORKING_FOLDER}"
