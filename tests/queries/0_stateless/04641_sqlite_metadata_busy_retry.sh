#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The SQLite metadata lookups (schema inference of the SQLite format, the table structure fetch of the
# sqlite table function, and the table checks of the SQLite database engine) must wait for a concurrent
# exclusive writer lock instead of failing immediately with "database is locked", the same way the scan
# paths retry SQLITE_BUSY on prepare and step.

DB_PATH="${CLICKHOUSE_TMP}/04641_sqlite_busy.db"
LOCK_HELD_FLAG="${CLICKHOUSE_TMP}/04641_lock_held.flag"
rm -f "${DB_PATH}" "${LOCK_HELD_FLAG}"

sqlite3 "${DB_PATH}" "CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (42);"

# Hold an exclusive lock on the database and signal once it is held; release it after a few seconds.
# The queries below start only after the lock is confirmed held, so without the retry every one of them
# would fail immediately with "database is locked" instead of waiting for the release.
python3 - "${DB_PATH}" "${LOCK_HELD_FLAG}" <<'EOF' &
import sqlite3
import sys
import time

connection = sqlite3.connect(sys.argv[1], timeout=30)
connection.execute("BEGIN EXCLUSIVE")
open(sys.argv[2], "w").close()
time.sleep(3)
connection.commit()
connection.close()
EOF
LOCKER_PID=$!

for _ in {1..300}; do
    [ -f "${LOCK_HELD_FLAG}" ] && break
    sleep 0.1
done

${CLICKHOUSE_LOCAL} --query "DESCRIBE file('${DB_PATH}', 'SQLite')" > "${CLICKHOUSE_TMP}/04641_out_describe.txt" 2>&1 &
QUERY_PID_1=$!
${CLICKHOUSE_LOCAL} --query "SELECT x FROM sqlite('${DB_PATH}', 't')" > "${CLICKHOUSE_TMP}/04641_out_function.txt" 2>&1 &
QUERY_PID_2=$!
${CLICKHOUSE_LOCAL} --query "CREATE DATABASE sqlite_04641 ENGINE = SQLite('${DB_PATH}'); SHOW TABLES FROM sqlite_04641; SELECT x FROM sqlite_04641.t" > "${CLICKHOUSE_TMP}/04641_out_database.txt" 2>&1 &
QUERY_PID_3=$!

wait "${QUERY_PID_1}" "${QUERY_PID_2}" "${QUERY_PID_3}" "${LOCKER_PID}"

echo "DESCRIBE file over a locked database:"
cat "${CLICKHOUSE_TMP}/04641_out_describe.txt"
echo "sqlite table function over a locked database:"
cat "${CLICKHOUSE_TMP}/04641_out_function.txt"
echo "SQLite database engine over a locked database:"
cat "${CLICKHOUSE_TMP}/04641_out_database.txt"

rm -f "${DB_PATH}" "${LOCK_HELD_FLAG}" "${CLICKHOUSE_TMP}"/04641_out_*.txt
