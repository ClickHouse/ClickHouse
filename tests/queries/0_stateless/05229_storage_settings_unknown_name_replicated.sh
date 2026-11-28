#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: this test explicitly creates a Replicated database.

# In a `Replicated` database the initiator holds a metadata transaction, and so does every secondary
# replaying the DDL, so the presence of a transaction cannot be what decides whether a definition may be
# refused - only `isInitialQuery()` can. Refusing on a secondary what the initiator committed would make
# it retry its queue entry forever, and a definition already committed by an older server must still
# replay onto a newer one.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_r"
tbl_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
CREATE DATABASE ${db}
    ENGINE = Replicated('/test/{database}/storage_settings_unknown_name', 'shard1', 'replica1');"

# The initiator judges its own DDL even though it holds a metadata transaction: nothing that a secondary
# would have to refuse is enqueued in the first place.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${db}.t (a UInt64) ENGINE = TinyLog SETTINGS not_a_setting_at_all = 1
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'create_rejected'

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_explicit_uuid=2 -q "
    ATTACH TABLE ${db}.t UUID '${tbl_uuid}' (a UInt64) ENGINE = TinyLog SETTINGS not_a_setting_at_all = 1
" 2>&1 | grep -q 'UNKNOWN_SETTING' && echo 'attach_rejected'

# A definition naming only real settings still goes through the whole replicated path.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${db}.ok (a UInt64) ENGINE = TinyLog SETTINGS disk = 'default'"
${CLICKHOUSE_CLIENT} -q "SELECT 'accepted', name FROM system.tables WHERE database = '${db}' AND name = 'ok'"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${db} SYNC"
