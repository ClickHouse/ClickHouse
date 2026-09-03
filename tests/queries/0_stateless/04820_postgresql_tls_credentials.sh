#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: the PostgreSQL integration is not available in the fast test build.
# no-parallel, no-replicated-database: a named collection is used.

# TLS credentials of a PostgreSQL source that are given as the contents of a certificate or a key
# file (`sslrootcert_pem`, `sslcert_pem`, `sslkey_pem`) must be redacted as [HIDDEN] when a query is
# formatted -- for the `postgresql` table function, the `PostgreSQL` table and database engines, the
# `MaterializedPostgreSQL` table and database engines and the PostgreSQL dictionary source -- while
# the arguments around them stay visible.
#
# A path to such a file (`sslrootcert`, `sslcert`, `sslkey`) is only accepted from a named collection
# defined in the server configuration file, because the server opens the file with its own
# privileges. Rejecting it needs no PostgreSQL server: it happens while the arguments are parsed.
# The rejection of a query override of a configuration-defined collection needs one, and lives in
# tests/integration/test_postgresql_ssl.

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
format "postgresql table function" \
    "SELECT * FROM postgresql(creds, sslrootcert_pem = '${SECRET}', table = 't', sslkey_pem = '${SECRET}', password = '${SECRET}')"
format "PostgreSQL table engine" \
    "CREATE TABLE t (x Int32) ENGINE = PostgreSQL(creds, sslcert_pem = '${SECRET}', table = 't', sslkey_pem = '${SECRET}')"
format "PostgreSQL database engine" \
    "CREATE DATABASE d ENGINE = PostgreSQL(creds, sslrootcert_pem = '${SECRET}', database = 'db', sslkey_pem = '${SECRET}')"
format "MaterializedPostgreSQL database engine" \
    "CREATE DATABASE d ENGINE = MaterializedPostgreSQL(creds, sslrootcert_pem = '${SECRET}', database = 'db', sslkey_pem = '${SECRET}')"
format "PostgreSQL dictionary source" \
    "CREATE DICTIONARY d (id UInt32) PRIMARY KEY id SOURCE(POSTGRESQL(NAME creds DB 'db' SSLCERT_PEM '${SECRET}' SSLKEY_PEM '${SECRET}')) LAYOUT(FLAT()) LIFETIME(0)"

# The same without a named collection: the credentials follow the positional arguments.
format "postgresql table function, positional arguments" \
    "SELECT * FROM postgresql('127.0.0.1:5432', 'db', 't', 'u', '${SECRET}', sslmode = 'verify-full', sslrootcert_pem = '${SECRET}', sslkey_pem = '${SECRET}')"
format "PostgreSQL table engine, positional arguments" \
    "CREATE TABLE t (x Int32) ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 't', 'u', '${SECRET}', sslcert_pem = '${SECRET}')"
format "PostgreSQL database engine, positional arguments" \
    "CREATE DATABASE d ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'u', '${SECRET}', sslrootcert_pem = '${SECRET}')"
format "MaterializedPostgreSQL table engine, positional arguments" \
    "CREATE TABLE t (x Int32) ENGINE = MaterializedPostgreSQL('127.0.0.1:5432', 'db', 't', 'u', '${SECRET}', sslrootcert_pem = '${SECRET}') ORDER BY x"

# The key of a named argument is not required to be a plain identifier or literal: the named
# collection parser evaluates it as a constant expression, so `concat('sslrootcert', '_pem')` names a
# TLS credential too. The formatter cannot evaluate it, so it hides the value of every argument whose
# key it cannot read; the keys themselves and the arguments around them stay visible.
format "key given as a constant expression" \
    "SELECT * FROM postgresql(creds, concat('sslrootcert', '_pem') = '${SECRET}', table = 't')"
format "key given as a constant expression, positional arguments" \
    "SELECT * FROM postgresql('127.0.0.1:5432', 'db', 't', 'u', '${SECRET}', upper('sslkey_pem') = '${SECRET}')"

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
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS postgresql_04820"
$CLICKHOUSE_CLIENT --query "
    CREATE NAMED COLLECTION postgresql_04820 AS
        host = '127.0.0.1', port = 5432, user = 'u', password = 'p', database = 'd', sslrootcert = '/etc/ssl/certs/ca.crt'"

MESSAGE="can only be specified in a named collection defined in the server configuration file"

# Stored in a collection created with SQL.
expect_error "$MESSAGE" $CLICKHOUSE_CLIENT --query "SELECT * FROM postgresql(postgresql_04820, table = 't')"

# Passed as a query argument.
for key in sslrootcert sslcert sslkey; do
    expect_error "$MESSAGE" $CLICKHOUSE_CLIENT --query \
        "SELECT * FROM postgresql(postgresql_04820, table = 't', ${key} = '/etc/ssl/certs/ca.crt')"
done

# Passed as a query argument without a named collection.
for key in sslrootcert sslcert sslkey; do
    expect_error "$MESSAGE" $CLICKHOUSE_CLIENT --query \
        "SELECT * FROM postgresql('127.0.0.1:5432', 'd', 't', 'u', 'p', ${key} = '/etc/ssl/certs/ca.crt')"
    expect_error "$MESSAGE" $CLICKHOUSE_CLIENT --query \
        "CREATE DATABASE db_04820 ENGINE = PostgreSQL('127.0.0.1:5432', 'd', 'u', 'p', ${key} = '/etc/ssl/certs/ca.crt')"
done

# The contents are accepted in the same place: the query gets as far as connecting, which is a
# different error than a complaint about the number of arguments.
echo "--- contents are accepted as a positional argument"
if $CLICKHOUSE_CLIENT --query \
    "SELECT * FROM postgresql('127.0.0.1:1', 'd', 't', 'u', 'p', sslmode = 'require', sslrootcert_pem = 'not a certificate')" 2>&1 \
    | grep -q "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"; then
    echo "FAIL: the credentials were not recognized as arguments"
else
    echo "OK"
fi

# In a dictionary created with a DDL query. The source of a dictionary is instantiated when it is
# loaded, so the reload is what surfaces the rejection if the `CREATE` itself did not.
$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_04820"
expect_error "cannot be specified in a dictionary created with a DDL query" bash -c "
    $CLICKHOUSE_CLIENT --query \"
        CREATE DICTIONARY dict_04820 (id UInt32, name String) PRIMARY KEY id
        SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 5432 USER 'u' PASSWORD 'p' DB 'd' TABLE 't'
                          SSLROOTCERT '/etc/ssl/certs/ca.crt'))
        LAYOUT(FLAT()) LIFETIME(0)\"
    $CLICKHOUSE_CLIENT --query 'SYSTEM RELOAD DICTIONARY dict_04820'"

$CLICKHOUSE_CLIENT --query "DROP DICTIONARY IF EXISTS dict_04820"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS db_04820"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION postgresql_04820"
