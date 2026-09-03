#!/usr/bin/env bash
# Tags: no-fasttest
# Reason: needs the MySQL and PostgreSQL integrations, which are not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The address expression is expanded while the arguments are parsed, long before anything is
# connected to, so nothing here reaches the network. The MySQL and PostgreSQL configuration helpers
# are shared by the table functions, the table engines and the database engines, and each of these
# surfaces has to report itself instead of the name of the argument it happens to share.

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --query "SELECT * FROM mysql('127.0.0.{1..20}:3306', 'db', 'tbl', 'u', 'p')" 2>&1 \
    | grep -oF -e "Table function 'mysql'" -e "too many result addresses: 20, while at most 10 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.mysql_glob (x UInt8) ENGINE = MySQL('127.0.0.{1..20}:3306', 'db', 'tbl', 'u', 'p')" 2>&1 \
    | grep -oF -e "Table engine 'MySQL'" -e "too many result addresses: 20, while at most 10 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --query "SELECT * FROM postgresql('127.0.0.{1..20}:5432', 'db', 'tbl', 'u', 'p')" 2>&1 \
    | grep -oF -e "Table function 'postgresql'" -e "too many result addresses: 20, while at most 10 are allowed" -e "'glob_expansion_max_elements' setting" \
    | head -n 3

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.postgres_glob (x UInt8) ENGINE = PostgreSQL('127.0.0.{1..20}:5432', 'db', 'tbl', 'u', 'p')" 2>&1 \
    | grep -oF -e "Table engine 'PostgreSQL'" -e "too many result addresses: 20, while at most 10 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --allow_experimental_materialized_postgresql_table 1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}.materialized_postgres_glob (x UInt8) ENGINE = MaterializedPostgreSQL('127.0.0.{1..20}:5432', 'db', 'tbl', 'u', 'p') ORDER BY x" 2>&1 \
    | grep -oF -e "Table engine 'MaterializedPostgreSQL'" -e "too many result addresses: 20, while at most 10 are allowed" \
    | head -n 2

# The database engines share the same configuration helpers, and they too expand the address
# expression while the `CREATE DATABASE` arguments are parsed, before a connection is attempted.
$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_mysql_glob ENGINE = MySQL('127.0.0.{1..20}:3306', 'db', 'u', 'p')" 2>&1 \
    | grep -oF -e "Database engine 'MySQL'" -e "too many result addresses: 20, while at most 10 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_postgres_glob ENGINE = PostgreSQL('127.0.0.{1..20}:5432', 'db', 'u', 'p')" 2>&1 \
    | grep -oF -e "Database engine 'PostgreSQL'" -e "too many result addresses: 20, while at most 10 are allowed" \
    | head -n 2

# `MaterializedPostgreSQL` accepts an address expression only through a named collection: its
# positional syntax parses a single `host:port` with `parseAddress`.
NAMED_COLLECTION="glob_addresses_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION IF EXISTS ${NAMED_COLLECTION}"
$CLICKHOUSE_CLIENT --query "CREATE NAMED COLLECTION ${NAMED_COLLECTION} AS addresses_expr = '127.0.0.{1..20}:5432', database = 'db', user = 'u', password = 'p'"

$CLICKHOUSE_CLIENT --glob_expansion_max_elements 10 --allow_experimental_database_materialized_postgresql 1 --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_materialized_postgres_glob ENGINE = MaterializedPostgreSQL(${NAMED_COLLECTION})" 2>&1 \
    | grep -oF -e "Database engine 'MaterializedPostgreSQL'" -e "too many result addresses: 20, while at most 10 are allowed" \
    | head -n 2

$CLICKHOUSE_CLIENT --query "DROP NAMED COLLECTION ${NAMED_COLLECTION}"
