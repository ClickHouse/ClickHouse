#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: this test explicitly creates a Replicated database.

# In a `Replicated` database the initiator holds a metadata transaction, and so does every secondary
# replaying the DDL, so the presence of a transaction cannot be what decides whether a column `SETTINGS`
# name may be refused - only `isInitialQuery()` can. Written as `metadata_txn != nullptr`, the exemption
# would cover the initiator too and every unknown name would be silently dropped again, with the rest of
# the suite green: the other tests for this check run on an `Atomic` database, where there is no
# transaction at all.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_r"
tbl_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
CREATE DATABASE ${db}
    ENGINE = Replicated('/test/{database}/column_settings_unknown_name', 'shard1', 'replica1');"

# The initiator judges its own definition even though it holds a metadata transaction.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${db}.t (x UInt64 SETTINGS (not_a_setting = DEFAULT)) ENGINE = MergeTree ORDER BY x
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'create_default_rejected'

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${db}.t (x UInt64 SETTINGS (param_not_a_setting = 1)) ENGINE = MergeTree ORDER BY x
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'create_parameter_rejected'

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_explicit_uuid=2 -q "
    ATTACH TABLE ${db}.t UUID '${tbl_uuid}' (x UInt64 SETTINGS (not_a_setting = DEFAULT))
    ENGINE = MergeTree ORDER BY x
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'attach_rejected'

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${db}.ok (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x"

# An ALTER is enqueued before the initiator validates it, so these also show that a refused entry does
# not stall the queue behind it.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    ALTER TABLE ${db}.ok ADD COLUMN a UInt64 SETTINGS (not_a_setting = DEFAULT)
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'add_column_rejected'

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    ALTER TABLE ${db}.ok MODIFY COLUMN y UInt64 SETTINGS (param_not_a_setting = 1)
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'modify_column_rejected'

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    ALTER TABLE ${db}.ok MODIFY COLUMN y RESET SETTING not_a_setting
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'reset_setting_rejected'

# A settable name still goes through the whole replicated path.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    ALTER TABLE ${db}.ok MODIFY COLUMN y UInt64 SETTINGS (min_compress_block_size = 100)"
${CLICKHOUSE_CLIENT} -q "
SELECT 'accepted', create_table_query LIKE '%\`y\` UInt64 SETTINGS (min_compress_block_size = 100)%'
FROM system.tables WHERE database = '${db}' AND name = 'ok'"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${db} SYNC"
