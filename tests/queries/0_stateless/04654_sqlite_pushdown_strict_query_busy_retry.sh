#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The pushdown-safety probe (`isPushdownSafeColumn`: the STRICT-table check and the column metadata fetch)
# must wait for a concurrent exclusive writer lock like the other SQLite metadata paths, instead of failing
# closed. Failing closed would empty the pushdown-eligible column set, and with
# `external_table_strict_query = 1` the query would be rejected with `INCORRECT_QUERY` even though the
# filter is pushdown-safe and merely has to wait for the lock to be released.

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_busy.db"
LOCK_HELD_FLAG="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_busy.flag"
OUT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_busy_out.txt"
LOG_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_busy_log.txt"
trap 'rm -f "$DB_PATH" "$LOCK_HELD_FLAG" "$OUT_FILE" "$LOG_FILE"' EXIT
rm -f "$DB_PATH" "$LOCK_HELD_FLAG"

sqlite3 "$DB_PATH" "CREATE TABLE t (x INTEGER NOT NULL) STRICT; INSERT INTO t VALUES (1), (42);"

# Hold an exclusive lock on the database and signal once it is held; release it after a few seconds. The
# query below starts only after the lock is confirmed held, so without the busy-retry in the pushdown probe
# it would fail immediately instead of waiting for the release.
python3 - "$DB_PATH" "$LOCK_HELD_FLAG" <<'EOF' &
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
    [ -f "$LOCK_HELD_FLAG" ] && break
    sleep 0.1
done

${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
    CREATE TABLE t (x Int64) ENGINE = SQLite('$DB_PATH', 't');
    SET external_table_strict_query = 1;
    SELECT x FROM t WHERE x = 42;
" > "$OUT_FILE" 2> "$LOG_FILE"

wait "$LOCKER_PID"

echo "Strict pushdown query over a locked database succeeds after the lock is released:"
cat "$OUT_FILE"
echo "Query sent to SQLite retains the pushed-down filter:"
grep -oE 'Query: SELECT `x` FROM `t`( WHERE .*)?$' "$LOG_FILE"
