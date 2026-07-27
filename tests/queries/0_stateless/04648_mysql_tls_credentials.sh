#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: the MySQL integration is not available in the fast test build.
# no-parallel, no-replicated-database: a named collection is used.

# TLS credentials of a MySQL source that are given as the contents of a certificate or a key file
# (`ssl_ca_pem`, `ssl_cert_pem`, `ssl_key_pem`) must be redacted as [HIDDEN] when a query is
# formatted -- for the `mysql` table function, the `MySQL` table and database engines and the MySQL
# dictionary source -- while the arguments around them stay visible.
#
# A path to such a file (`ssl_ca`, `ssl_cert`, `ssl_key`) is only accepted from a named collection
# defined in the server configuration file, because the server opens the file with its own
# privileges. Rejecting it needs no MySQL server: it happens while the arguments are parsed.
# The rejection of a query override of a configuration-defined collection needs one, and lives in
# tests/integration/test_storage_mysql.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SECRET="SECRET_THAT_MUST_NOT_LEAK"

format() {
    echo "--- $1"
    local formatted
    formatted=$(echo "$2" | $CLICKHOUSE_FORMAT --oneline)
    echo "$formatted"
    if echo "$formatted" | grep -q "$SECRET"; then
        echo "FAIL: secret leaked in formatted query"
    fi
}

# `table`, `database` and `db` sit in between two credentials on purpose: hiding several arguments
# must not hide what is between them.
format "mysql table function" \
    "SELECT * FROM mysql(creds, ssl_ca_pem = '${SECRET}', table = 't', ssl_key_pem = '${SECRET}', password = '${SECRET}')"
format "MySQL table engine" \
    "CREATE TABLE t (x Int32) ENGINE = MySQL(creds, ssl_cert_pem = '${SECRET}', table = 't', ssl_key_pem = '${SECRET}')"
format "MySQL database engine" \
    "CREATE DATABASE d ENGINE = MySQL(creds, ssl_ca_pem = '${SECRET}', database = 'db', ssl_key_pem = '${SECRET}')"
format "MySQL dictionary source" \
    "CREATE DICTIONARY d (id UInt32) PRIMARY KEY id SOURCE(MYSQL(NAME creds DB 'db' SSL_CERT_PEM '${SECRET}' SSL_KEY_PEM '${SECRET}')) LAYOUT(FLAT()) LIFETIME(0)"

expect_error() {
    local pattern="$1"
    shift
    if "$@" 2>&1 | grep -q "$pattern"; then
        echo "OK"
    else
        echo "FAIL: expected an error matching: $pattern"
    fi
}

echo "--- paths from SQL are rejected"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS mysql_04648"
$CLICKHOUSE_CLIENT --query "
    CREATE NAMED COLLECTION mysql_04648 AS
        host = '127.0.0.1', port = 3306, user = 'u', password = 'p', database = 'd', ssl_ca = '/etc/ssl/certs/ca.crt'"

MESSAGE="can only be specified in a named collection defined in the server configuration file"

# Stored in a collection created with SQL.
expect_error "$MESSAGE" $CLICKHOUSE_CLIENT --query "SELECT * FROM mysql(mysql_04648, table = 't')"

# Passed as a query argument.
for key in ssl_ca ssl_cert ssl_key; do
    expect_error "$MESSAGE" $CLICKHOUSE_CLIENT --query \
        "SELECT * FROM mysql(mysql_04648, table = 't', ${key} = '/etc/ssl/certs/ca.crt')"
done

# In a dictionary created with a DDL query. The source of a dictionary is instantiated when it is
# loaded, so the reload is what surfaces the rejection if the `CREATE` itself did not.
$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_04648"
expect_error "cannot be specified in a dictionary created with a DDL query" bash -c "
    $CLICKHOUSE_CLIENT --query \"
        CREATE DICTIONARY dict_04648 (id UInt32, name String) PRIMARY KEY id
        SOURCE(MYSQL(HOST '127.0.0.1' PORT 3306 USER 'u' PASSWORD 'p' DB 'd' TABLE 't'
                     SSL_CA '/etc/ssl/certs/ca.crt'))
        LAYOUT(FLAT()) LIFETIME(0)\"
    $CLICKHOUSE_CLIENT --query 'SYSTEM RELOAD DICTIONARY dict_04648'"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_04648"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION mysql_04648"
