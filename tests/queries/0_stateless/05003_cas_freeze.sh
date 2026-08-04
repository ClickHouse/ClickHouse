#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# CA durability oracle: a FREEZE PARTITION snapshot is an independent GC root — it survives
# ALTER TABLE ... DROP PARTITION on the same partition and remains independently recoverable.
# This property is CA-specific: on a plain disk FREEZE makes a hard-link snapshot in shadow/ which
# is independent by construction; on a CA disk the frozen part must be written as a separate shadow
# ref (not merely an alias of the live part ref) so DROP PARTITION cannot destroy it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UNFREEZE_STRUCTURE='command_type String, partition_id String, part_name String, backup_name String, backup_path String, part_backup_path String'

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_cas_freeze;"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_cas_freeze (k UInt32, v String)
ENGINE = MergeTree ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05003',
    name = '05003_cas_freeze',
    path = '05003_cas_freeze_pool/');"

# Two partitions: k=1 (will be frozen then dropped) and k=2 (must survive untouched).
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_cas_freeze;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_freeze VALUES (1, 'a'), (1, 'b'), (1, 'c');"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_cas_freeze VALUES (2, 'x'), (2, 'y');"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_cas_freeze;"

${CLICKHOUSE_CLIENT} --query "SELECT 'live_before_freeze', k, count() FROM t_cas_freeze GROUP BY k ORDER BY k;"

# Freeze only partition 1. The shadow ref becomes an independent GC root on the CA disk.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cas_freeze FREEZE PARTITION 1 WITH NAME 'backup_05003';"

${CLICKHOUSE_CLIENT} --query "
SELECT 'is_frozen', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_cas_freeze'
  AND partition_id = '1' AND is_frozen AND active;"

# Drop the live partition 1. On a CA disk this must NOT remove the shadow ref.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE t_cas_freeze DROP PARTITION 1;"

# Live k=1 is gone; k=2 is untouched.
${CLICKHOUSE_CLIENT} --query "SELECT 'live_after_drop', k, count() FROM t_cas_freeze GROUP BY k ORDER BY k;"

# THE KEY ASSERTION: SYSTEM UNFREEZE finds and removes the frozen snapshot of partition 1,
# proving it survived the DROP PARTITION as an independent shadow ref.
# SYSTEM UNFREEZE does not accept a FORMAT clause; default output is TSV, piped through
# clickhouse-local to filter to deterministic columns (backup_path/part_backup_path are
# absolute paths; command_type/partition_id/part_name/backup_name are stable).
${CLICKHOUSE_CLIENT} --query "SYSTEM UNFREEZE WITH NAME 'backup_05003';" \
  | ${CLICKHOUSE_LOCAL} --structure "$UNFREEZE_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_cas_freeze;"
${CLICKHOUSE_CLIENT} --query "SELECT 'dropped_ok';"
