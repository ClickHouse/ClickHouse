#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage for RestoreSettings::RestoreTableCreationMode string/integer parsing
# in RestoreSettings.cpp: exercises the 'create', 'if_not_exists', and
# 'must_exist' code paths in SettingFieldRestoreTableCreationMode::setValue().

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_DATABASE}_04149_restore_settings"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_restore_src;
CREATE TABLE t_restore_src (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_restore_src VALUES (1), (2), (3);
"

$CLICKHOUSE_CLIENT --query "BACKUP TABLE t_restore_src TO File('${BACKUP_NAME}.zip')" --format Null

# ---------------------------------------------------------------------------
# create_table = 'create'  — restore to a fresh name (exercises kCreate path).
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_restore_create;"
$CLICKHOUSE_CLIENT \
    --query "RESTORE TABLE t_restore_src AS t_restore_create FROM File('${BACKUP_NAME}.zip') SETTINGS create_table = 'create'" \
    --format Null
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_restore_create;"

# ---------------------------------------------------------------------------
# create_table = 'if_not_exists' — table absent, should create it.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_restore_ifne;"
$CLICKHOUSE_CLIENT \
    --query "RESTORE TABLE t_restore_src AS t_restore_ifne FROM File('${BACKUP_NAME}.zip') SETTINGS create_table = 'if_not_exists'" \
    --format Null
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_restore_ifne;"

# ---------------------------------------------------------------------------
# create_table = 'must_exist' — pre-create table, then restore into it.
# ---------------------------------------------------------------------------
$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_restore_me;
CREATE TABLE t_restore_me (x UInt32) ENGINE = MergeTree ORDER BY x;
"
$CLICKHOUSE_CLIENT \
    --query "RESTORE TABLE t_restore_src AS t_restore_me FROM File('${BACKUP_NAME}.zip') SETTINGS create_table = 'must_exist'" \
    --format Null
$CLICKHOUSE_CLIENT --query "SELECT count() FROM t_restore_me;"

# Cleanup
$CLICKHOUSE_CLIENT --query "
DROP TABLE t_restore_src;
DROP TABLE t_restore_create;
DROP TABLE t_restore_ifne;
DROP TABLE t_restore_me;
"
