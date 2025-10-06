#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config=$CUR_DIR/03642_clickhouse_local_database_disk.local.yaml

export CLICKHOUSE_LOCAL_DISK_PATH=$CLICKHOUSE_TEST_UNIQUE_NAME/local-disk/
path=$CLICKHOUSE_TEST_UNIQUE_NAME/local-database/

system_database_uuid=$($CLICKHOUSE_LOCAL -q "select generateUUIDv4()")

mkdir -p "$CLICKHOUSE_LOCAL_DISK_PATH/metadata"
cat > "$CLICKHOUSE_LOCAL_DISK_PATH/metadata/system.sql" <<EOL
ATTACH DATABASE _ UUID '$system_database_uuid'
ENGINE = Atomic
EOL

$CLICKHOUSE_LOCAL --config-file "$config" --path "$path" -q "create table system.foo (key Int) engine=Null()"
$CLICKHOUSE_LOCAL --config-file "$config" --path "$path" -q "show create system.foo"
$CLICKHOUSE_LOCAL --config-file "$config" --path "$path" --only-system-tables -q "show create system.foo"

rm -fr "${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
