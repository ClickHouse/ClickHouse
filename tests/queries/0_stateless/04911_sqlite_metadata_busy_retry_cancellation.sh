#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: requires the SQLite library, which is not built in the fast test.
# no-parallel: this test deliberately holds an exclusive SQLite database lock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_NAME="${CLICKHOUSE_DATABASE}_sqlite_metadata_busy_retry.db"
DB_PATH="${USER_FILES_PATH}/${DB_NAME}"
LOCK_HELD_FLAG="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_metadata_busy_retry.flag"
OUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_metadata_busy_retry.out"
QUERY_ID="sqlite-metadata-busy-retry-${CLICKHOUSE_DATABASE}-${RANDOM}"
LOCKER_PID=""

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${QUERY_ID}' SYNC FORMAT Null" 2>/dev/null ||:
    if [ -n "${LOCKER_PID}" ]
    then
        kill "${LOCKER_PID}" 2>/dev/null ||:
        wait "${LOCKER_PID}" 2>/dev/null ||:
    fi
    rm -f "${DB_PATH}" "${LOCK_HELD_FLAG}" "${OUT_FILE}"
}
trap cleanup EXIT

rm -f "${DB_PATH}" "${LOCK_HELD_FLAG}" "${OUT_FILE}"
sqlite3 "${DB_PATH}" "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);"
chmod ugo+w "${DB_PATH}"

# Schema inference has to prepare and step SQLite metadata statements. Keep them blocked by an exclusive
# writer lock until the query is killed.
python3 - "${DB_PATH}" "${LOCK_HELD_FLAG}" <<'EOF' &
import sqlite3
import sys
import time

connection = sqlite3.connect(sys.argv[1], timeout=30)
connection.execute("BEGIN EXCLUSIVE")
open(sys.argv[2], "w").close()
time.sleep(30)
connection.commit()
connection.close()
EOF
LOCKER_PID=$!

for _ in {1..300}
do
    [ -f "${LOCK_HELD_FLAG}" ] && break
    sleep 0.1
done
[[ -f "${LOCK_HELD_FLAG}" ]] || { echo "SQLite exclusive lock was not acquired"; exit 1; }

${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" --query "DESCRIBE file('${DB_NAME}', SQLite)" 2>&1 \
    | grep -o -m 1 'QUERY_WAS_CANCELLED' > "${OUT_FILE}" &
QUERY_PID=$!

for _ in {1..100}
do
    result=$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.processes WHERE query_id = '${QUERY_ID}'")
    [[ ${result} == "1" ]] && break
    sleep 0.1
done
[[ ${result} == "1" ]] || { echo "metadata query did not start"; exit 1; }

${CLICKHOUSE_CLIENT} --query "KILL QUERY WHERE query_id = '${QUERY_ID}' SYNC FORMAT Null"
wait "${QUERY_PID}" || { echo "metadata query was not cancelled with QUERY_WAS_CANCELLED"; exit 1; }

echo 'Cancelling a metadata query blocked on SQLite lock reports:'
cat "${OUT_FILE}"
