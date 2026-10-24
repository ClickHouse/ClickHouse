#!/usr/bin/env bash
# Tags: no-fasttest
# `PARALLEL WITH` runs its subqueries as internal ones. A user `CREATE DATABASE ... ENGINE = DataLakeCatalog`
# wrapped in it must still build the catalog eagerly, i.e. be rejected just like the plain `CREATE` when the
# catalog would rely on the server's own credentials. `RESTORE` keeps deferring catalog construction.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_dlc"
db_other="${CLICKHOUSE_DATABASE}_other"
db_restored="${CLICKHOUSE_DATABASE}_dlc_restored"
engine="DataLakeCatalog('http://127.0.0.1:1/catalog') SETTINGS catalog_type = 'glue', region = 'us-east-1'"
settings="--allow_database_glue_catalog 1 --s3_allow_server_credentials_in_user_queries 0"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db; DROP DATABASE IF EXISTS $db_other; DROP DATABASE IF EXISTS $db_restored"
}
cleanup

echo "--- plain CREATE"
$CLICKHOUSE_CLIENT $settings -q "CREATE DATABASE $db ENGINE = $engine" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "--- CREATE wrapped in PARALLEL WITH"
$CLICKHOUSE_CLIENT $settings -q "CREATE DATABASE $db_other ENGINE = Memory PARALLEL WITH CREATE DATABASE $db ENGINE = $engine" 2>&1 | grep -o -m1 "ACCESS_DENIED"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db'"

echo "--- RESTORE defers catalog construction"
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
$CLICKHOUSE_CLIENT --allow_database_glue_catalog 1 --s3_allow_server_credentials_in_user_queries 1 -q "CREATE DATABASE $db ENGINE = $engine"
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE $db TO $backup FORMAT Null"
$CLICKHOUSE_CLIENT $settings -q "RESTORE DATABASE $db AS $db_restored FROM $backup FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '$db_restored'"

cleanup
