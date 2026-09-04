#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: Unsupported type of ALTER query

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ALTER_OUT_STRUCTURE='command_type String, partition_id String, part_name String'
ATTACH_OUT_STRUCTURE='old_part_name String'
FREEZE_OUT_STRUCTURE='backup_name String, backup_path String , part_backup_path String'

# FREEZE/UNFREEZE "WITH NAME" is a process-global backup name (not scoped to a
# database or table), so make every backup name unique per test run and mask
# the suffix back out below to keep the reference output stable.
UNIQUE_SUFFIX="$CLICKHOUSE_TEST_UNIQUE_NAME"

# setup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS table_for_freeze;"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE table_for_freeze (key UInt64, value String) ENGINE = MergeTree() ORDER BY key PARTITION BY key % 10;"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze SELECT number, toString(number) from numbers(10);"

# also for old syntax
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS table_for_freeze_old_syntax;"
${CLICKHOUSE_CLIENT} --allow_deprecated_syntax_for_merge_tree=1 --query "CREATE TABLE table_for_freeze_old_syntax (dt Date, value String) ENGINE = MergeTree(dt, (value), 8192);"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze_old_syntax SELECT toDate('2021-03-01'), toString(number) from numbers(10);"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze FREEZE WITH NAME 'test_01417_$UNIQUE_SUFFIX' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze FREEZE PARTITION '3' WITH NAME 'test_01417_single_part_$UNIQUE_SUFFIX' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze DETACH PARTITION '3';"
${CLICKHOUSE_CLIENT} --query "INSERT INTO table_for_freeze VALUES (3, '3');"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze ATTACH PARTITION '3' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $ATTACH_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, old_part_name FROM table ORDER BY partition_id FORMAT TSVWithNames"


${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze DETACH PARTITION '5';"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze FREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7_$UNIQUE_SUFFIX', ATTACH PART '5_6_6_0' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE, $ATTACH_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name, old_part_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

# Unfreeze partition
${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze UNFREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7_$UNIQUE_SUFFIX' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

# Freeze partition with old syntax
${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze_old_syntax FREEZE PARTITION '202103' WITH NAME 'test_01417_single_part_old_syntax_$UNIQUE_SUFFIX' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

# Unfreeze partition with old syntax
${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze_old_syntax UNFREEZE PARTITION '202103' WITH NAME 'test_01417_single_part_old_syntax_$UNIQUE_SUFFIX' FORMAT TSVWithNames SETTINGS alter_partition_verbose_result = 1;" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table ORDER BY partition_id FORMAT TSVWithNames"

# Unfreeze the whole backup with SYSTEM query
${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze FREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7_system_$UNIQUE_SUFFIX'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE table_for_freeze"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE table_for_freeze UNFREEZE PARTITION '7' WITH NAME 'test_01417_single_part_7_system_$UNIQUE_SUFFIX'" 2>/dev/null
rc=$?
if [ $rc -eq 0 ]; then
    echo "ALTER query shouldn't unfreeze removed table. Code: $rc"
    exit 1
fi
${CLICKHOUSE_CLIENT} --query "SYSTEM UNFREEZE WITH NAME 'test_01417_single_part_7_system_$UNIQUE_SUFFIX'" \
  | sed "s/_$UNIQUE_SUFFIX//" \
  | ${CLICKHOUSE_LOCAL} --structure "$ALTER_OUT_STRUCTURE, $FREEZE_OUT_STRUCTURE" \
      --query "SELECT command_type, partition_id, part_name, backup_name FROM table"

# teardown
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS table_for_freeze;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS table_for_freeze_old_syntax;"
