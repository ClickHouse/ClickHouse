#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A self-join reaches the storage twice with the same `StorageID`, so a qualifier such as the alias `b`
# cannot identify which of the two instances is being read. A predicate of the other side must therefore
# never end up in the remote query of this side - it has to be dropped as foreign and applied locally.

DB_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_self_join.db"
trap 'rm -f "$DB_PATH"' EXIT
rm -f "$DB_PATH"

sqlite3 "$DB_PATH" "CREATE TABLE t (x INTEGER NOT NULL, y INTEGER NOT NULL) STRICT; INSERT INTO t VALUES (1, 2), (1, 4), (3, 4);"

for enable_analyzer in 1 0; do
    echo "enable_analyzer = ${enable_analyzer}"

    echo "Self-join result:"
    ${CLICKHOUSE_LOCAL} --query="
        CREATE TABLE ext (x Int64, y Int64) ENGINE = SQLite('$DB_PATH', 't');
        SELECT a.x, b.y FROM ext AS a JOIN ext AS b ON a.x = b.x WHERE a.x = 1 AND b.y = 2
        ORDER BY a.x, b.y SETTINGS enable_analyzer = ${enable_analyzer};
    "

    echo "Remote queries:"
    ${CLICKHOUSE_LOCAL} --send_logs_level=trace --query="
        CREATE TABLE ext (x Int64, y Int64) ENGINE = SQLite('$DB_PATH', 't');
        SELECT a.x, b.y FROM ext AS a JOIN ext AS b ON a.x = b.x WHERE a.x = 1 AND b.y = 2
        ORDER BY a.x, b.y SETTINGS enable_analyzer = ${enable_analyzer};
    " 2>&1 >/dev/null | grep -oE 'Query: SELECT .* FROM `t`( WHERE .*)?$' | sed 's/^/  /' | sort

    echo "Both sides of the filter are still honored under external_table_strict_query:"
    ${CLICKHOUSE_LOCAL} --query="
        CREATE TABLE ext (x Int64, y Int64) ENGINE = SQLite('$DB_PATH', 't');
        SELECT a.x, b.y FROM ext AS a JOIN ext AS b ON a.x = b.x WHERE a.x = 1 AND b.y = 2
        ORDER BY a.x, b.y SETTINGS enable_analyzer = ${enable_analyzer}, external_table_strict_query = 1;
    "
done
