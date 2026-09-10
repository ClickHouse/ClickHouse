#!/usr/bin/env bash
# Test the jdbc DataLakeCatalog settings without a live Postgres database.
# CREATE DATABASE connects eagerly, so the statement is validated with
# clickhouse-format; behavior against a real JdbcCatalog database is covered
# by live testing, not by stateless tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "--- CREATE DATABASE accepts the jdbc settings ---"
echo "CREATE DATABASE test_jdbc ENGINE=DataLakeCatalog SETTINGS catalog_type='jdbc', warehouse='jdbc', jdbc_host='postgres', jdbc_port=5432, jdbc_database='catalog', jdbc_schema='public', jdbc_user='reader', jdbc_password='secret'" \
    | $CLICKHOUSE_FORMAT --oneline | grep -o "catalog_type = 'jdbc'.*jdbc_user = 'reader'"

echo "--- jdbc_password is hidden when formatted ---"
echo "CREATE DATABASE test_jdbc ENGINE=DataLakeCatalog SETTINGS catalog_type='jdbc', warehouse='jdbc', jdbc_password='secret'" \
    | $CLICKHOUSE_FORMAT --oneline | grep -oE "(^|[, ])jdbc_password = '[^']*'" | sed -E "s/^[, ]//"
