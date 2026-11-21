#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database
# no-replicated-database: this test explicitly creates a Replicated database.

# A refreshable materialized view (no APPEND) in a Replicated database with a
# non-replicated target is structurally invalid: the storage constructor must reject it
# when it is defined, but such an MV can exist (its target can be replaced after the MV was created).
# A backup is user input, so `RESTORE` validates it like `CREATE`.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}_r"
mv_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

# Note that `target_tbl` is not replicated.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none <<EOF
CREATE DATABASE ${db}
    ENGINE = Replicated('/test/{database}/refreshable_mv_secondary_create', 'shard1', 'replica1');
CREATE TABLE ${db}.target_tbl (id Int64, val Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.target_tbl VALUES (1, 1.0), (2, 2.0);
EOF

# (1) A new definition of the bad combination is rejected, also by a full-definition ATTACH.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE MATERIALIZED VIEW ${db}.bad_mv
        REFRESH EVERY 100 YEAR
        TO ${db}.target_tbl
        (id Int64, val Float64)
        AS SELECT id, val FROM ${db}.target_tbl
" 2>&1 | grep -q "BAD_ARGUMENTS" && echo 'create_rejected'
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_explicit_uuid=2 -q "
    ATTACH MATERIALIZED VIEW ${db}.bad_mv UUID '${mv_uuid}'
        REFRESH EVERY 100 YEAR
        TO ${db}.target_tbl
        (id Int64, val Float64)
        AS SELECT id, val FROM ${db}.target_tbl
" 2>&1 | grep -q "BAD_ARGUMENTS" && echo 'attach_rejected'

# Get the MV into the database with a replicated target, then replace the target with the non-replicated one.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none <<EOF
RENAME TABLE ${db}.target_tbl TO ${db}.target_tbl_data;
CREATE TABLE ${db}.target_tbl (id Int64, val Float64) ENGINE = ReplicatedMergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${db}.bad_mv
    REFRESH EVERY 100 YEAR
    TO ${db}.target_tbl
    (id Int64, val Float64)
    AS SELECT id, val FROM ${db}.target_tbl;
DROP TABLE ${db}.target_tbl SYNC;
RENAME TABLE ${db}.target_tbl_data TO ${db}.target_tbl;
EOF

${CLICKHOUSE_CLIENT} -q "SELECT 'attached', count() FROM system.tables WHERE database = '${db}' AND name = 'bad_mv'"

# (2) `RESTORE` of a backup of the database is refused. Replicas loading it are covered by test_stored_definition_replay.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -m -q "
    BACKUP DATABASE ${db} TO Memory('bk') FORMAT Null;
    RESTORE DATABASE ${db} AS ${db}_restored FROM Memory('bk') FORMAT Null;
" 2>&1 | grep -q "BAD_ARGUMENTS" && echo 'restore_rejected'
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE IF EXISTS ${db}_restored SYNC"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${db} SYNC"
