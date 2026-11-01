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

echo "--- second run: the directory is gone; the database still loads and serves no tables ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store" \
    -q "SELECT name FROM system.databases WHERE name = 'fsdb';
        SELECT count() FROM system.tables WHERE database = 'fsdb'"

# The definition a start has just replayed must still be refused to a user who replays it by hand, plainly or
# wrapped in a clause that runs its children as internal queries. Each rejection ends its invocation, so they
# run one per start, and the start after each one replays the definition and attaches the database again.
echo "--- a user replay of that same definition is rejected (1 = the expected BAD_ARGUMENTS) ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store" \
    -q "DETACH DATABASE fsdb;
        ATTACH DATABASE fsdb" 2>&1 | grep -c "Code: 36.*BAD_ARGUMENTS"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store" \
    -q "DETACH DATABASE fsdb;
        ATTACH DATABASE fsdb PARALLEL WITH DROP TABLE IF EXISTS no_such_table_05136" 2>&1 \
    | grep -c "Code: 36.*BAD_ARGUMENTS"

echo "--- the database is still reachable through a replay on start, and drops ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store" \
    -q "SELECT name FROM system.databases WHERE name = 'fsdb';
        DROP DATABASE fsdb;
        SELECT count() FROM system.databases WHERE name = 'fsdb'"

# A path that stat cannot follow is as unreachable as a missing one, and the same start must survive it.
# The symlink to itself makes stat fail with ELOOP instead of with "no such file".
echo "--- a path that stat cannot follow: the database is created over a real directory ---"
mkdir -p "${WORKING_FOLDER}/loop"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store2" \
    -q "CREATE DATABASE fsdb2 ENGINE = Filesystem('${WORKING_FOLDER}/loop'); SELECT 'created'"
rm -rf "${WORKING_FOLDER}/loop"
ln -s "${WORKING_FOLDER}/loop" "${WORKING_FOLDER}/loop"

echo "--- the next start still loads it, serving no tables ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store2" \
    -q "SELECT name FROM system.databases WHERE name = 'fsdb2';
        SELECT count() FROM system.tables WHERE database = 'fsdb2'" 2>&1 | grep -o -E '^fsdb2$|^[0-9]+$|STD_EXCEPTION'

echo "--- a user replay of that definition is rejected (1 = the expected BAD_ARGUMENTS) ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store2" \
    -q "DETACH DATABASE fsdb2;
        ATTACH DATABASE fsdb2" 2>&1 | grep -c -E "Code: 36\..*Cannot access path.*BAD_ARGUMENTS"

echo "--- it is still reachable through a replay on start, and drops ---"
${CLICKHOUSE_LOCAL} --path "${WORKING_FOLDER}/store2" \
    -q "DROP DATABASE fsdb2;
        SELECT count() FROM system.databases WHERE name = 'fsdb2'" 2>&1 | grep -o -m1 -E '^[0-9]+$|STD_EXCEPTION'

# On a server a path stat fails on is refused the same way. A component over NAME_MAX makes stat fail without
# creating anything that a concurrent test globbing user_files could trip over.
LONG_NAME=$(printf '%*s' 300 '' | tr ' ' a)
DB_LONG="${CLICKHOUSE_DATABASE}_long"
echo "--- server: a path that stat refuses is rejected as such (1 = BAD_ARGUMENTS) ---"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB_LONG} ENGINE = Filesystem('${LONG_NAME}')" 2>&1 \
    | grep -m1 -c -E "Code: 36\..*Cannot access path.*BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB_LONG}"

rm -rf "${WORKING_FOLDER}"
