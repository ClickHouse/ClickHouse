#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `key = value` argument before the positional ones shifts them: every positional after it is hidden.
mixed_queries=(
    "SELECT * FROM mongodb(foo = 1, '127.0.0.1:27017', 'db', 'coll', 'user', 'leakmongo')"
    "SELECT * FROM redis(foo = 1, '127.0.0.1:6379', 'k', 'k String', 0, 'leakredisf')"
    "SELECT * FROM remote('127.0.0.1', foo = 1, 'db', 't', 'user', 'leakremote')"
    "CREATE TABLE tm1 (x Int32) ENGINE = ArrowFlight(foo = 1, '127.0.0.1:5006', 'ds', 'user', 'leakaf')"
    "CREATE TABLE tm3 (x Int32) ENGINE = ExternalDistributed(foo = 1, 'MySQL', '127.0.0.1:3306', 'db', 't', 'user', 'leakextdist')"
    "CREATE TABLE tm5 (x Int32) ENGINE = YTsaurus(foo = 1, 'http://127.0.0.1', '//p', 'leakyt')"
    "CREATE TABLE tm7 (x Int32) ENGINE = Redis(foo = 1, '127.0.0.1:6379', 0, 'leakredeng', 16) PRIMARY KEY x"
    "SELECT * FROM mysql(foo = 1, '127.0.0.1:3306', 'db', 't', 'user', 'leakmysql')"
)

# The all-positional forms hide only the password.
other_queries=(
    "SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'coll', 'user', 'pw')"
    "SELECT * FROM redis('127.0.0.1:6379', 'k', 'k String', 0, 'pw')"
    "SELECT * FROM remote('127.0.0.1', 'db', 't', 'user', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = ArrowFlight('127.0.0.1:5006', 'ds', 'user', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = ExternalDistributed('MySQL', '127.0.0.1:3306', 'db', 't', 'user', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = YTsaurus('http://127.0.0.1', '//p', 'pw')"
    "CREATE TABLE t (x Int32) ENGINE = Redis('127.0.0.1:6379', 0, 'pw', 16) PRIMARY KEY x"
    "SELECT * FROM mysql('127.0.0.1:3306', 'db', 't', 'user', 'pw')"
)

for query in "${mixed_queries[@]}" "${other_queries[@]}"; do
    $CLICKHOUSE_FORMAT --oneline <<< "$query"
done

# The rejected statements are logged with the same masking.
for query in "${mixed_queries[@]}"; do
    $CLICKHOUSE_CLIENT -q "$query" >/dev/null 2>&1
done
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS tm1; DROP TABLE IF EXISTS tm3; DROP TABLE IF EXISTS tm5; DROP TABLE IF EXISTS tm7"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM system.query_log
    WHERE current_database = currentDatabase() AND type != 'QueryStart' AND query LIKE '%leak%' AND query NOT LIKE '%query_log%'"
