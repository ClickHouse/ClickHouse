#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

database_name="$CLICKHOUSE_DATABASE"_03517_test_database
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $database_name SYNC"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $database_name ENGINE = Atomic"

$CLICKHOUSE_CLIENT -q "CREATE TABLE $database_name.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"

backup_path="$CLICKHOUSE_DATABASE"_03517_test_database_backup
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE $database_name TO Disk('disk_s3_plain_rewritable_03517', '$backup_path') FORMAT Null"
