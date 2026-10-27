#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage
# Backup with an archive extension to a plain_rewritable disk.
# The archive format causes root_path to be empty (parent_path of "name.zip"),
# which previously led to stack overflow during cleanup by traversing the entire disk.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup_name="${CLICKHOUSE_DATABASE}_03831_backup"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_backup_archive_plain SYNC"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_backup_archive_plain (x Int32) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_backup_archive_plain SELECT number FROM numbers(100)"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_backup_archive_plain TO Disk('disk_s3_plain_rewritable_03517', '${backup_name}.zip') FORMAT Null"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_restore_archive_plain SYNC"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_backup_archive_plain AS t_restore_archive_plain FROM Disk('disk_s3_plain_rewritable_03517', '${backup_name}.zip') FORMAT Null"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM t_restore_archive_plain"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_restore_archive_plain SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_backup_archive_plain SYNC"
