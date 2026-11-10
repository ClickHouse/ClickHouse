#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `FileLog` table reads files under `user_files_path`, the same data the `file` function and the
# `File` engine guard with the `FILE` source grant. The engine therefore declares `FILE` as its source,
# so `TABLE ENGINE ON FileLog` is no longer handed to everyone and a user without the source grant is
# refused at `CREATE TABLE` time instead of reading the staged files.

# A user and a file in `user_files` are global names, and the flaky check runs several copies of one
# test at once, so they carry the pid as well as the unique name.
NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"
USER="low_${NAME}"
# On 26.4 `FileLog` does not resolve a relative path against `user_files_path`, so pass it absolute.
FILE="${NAME}.csv"
TABLE="${CLICKHOUSE_DATABASE}.fl_${NAME}"

# Match the privilege sentence rather than the engine name: the client echoes the failing query, which
# itself contains `FileLog`.
denied() { grep -qF "necessary to have the grant TABLE ENGINE ON FileLog"; }

printf 'owned_marker\nsecret_payload\n' > "${USER_FILES_PATH:?}/${FILE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE, SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER}"

echo "-- no FILE grant: CREATE is refused, nothing is left behind"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${TABLE} (marker String) ENGINE = FileLog('${USER_FILES_PATH}/${FILE}', 'LineAsString')" 2>&1 | denied && echo "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}' AND name = 'fl_${NAME}'"

echo "-- READ and WRITE on FILE: CREATE and SELECT work"
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON FILE TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "CREATE TABLE ${TABLE} (marker String) ENGINE = FileLog('${USER_FILES_PATH}/${FILE}', 'LineAsString')"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SELECT marker FROM ${TABLE} ORDER BY marker SETTINGS stream_like_engine_allow_direct_select = 1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
rm -f "${USER_FILES_PATH:?}/${FILE}"
