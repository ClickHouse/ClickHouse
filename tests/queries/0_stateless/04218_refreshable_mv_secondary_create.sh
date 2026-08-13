#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database
# no-replicated-database: this test explicitly creates a Replicated database.

# A refreshable materialized view (no APPEND) in a Replicated database with a
# non-replicated target is structurally invalid: the storage constructor must reject it
# at `CREATE` strictness but accept it at `SECONDARY_CREATE` strictness because it is possible
# to create it via `ATTACH`. So we must be able to read it if someone has such an invalid MV in their database.

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

# (1) `CREATE` strictness: the bad combination is rejected.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE MATERIALIZED VIEW ${db}.bad_mv
        REFRESH EVERY 100 YEAR
        TO ${db}.target_tbl
        (id Int64, val Float64)
        AS SELECT id, val FROM ${db}.target_tbl
" 2>&1 | grep -q "BAD_ARGUMENTS" && echo 'create_rejected'

# Get the MV into the database via the `ATTACH` path.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_explicit_uuid=2 -q "
    ATTACH MATERIALIZED VIEW ${db}.bad_mv UUID '${mv_uuid}'
        REFRESH EVERY 100 YEAR
        TO ${db}.target_tbl
        (id Int64, val Float64)
        AS SELECT id, val FROM ${db}.target_tbl
"

${CLICKHOUSE_CLIENT} -q "SELECT 'attached', count() FROM system.tables WHERE database = '${db}' AND name = 'bad_mv'"

# (2) Round-trip through `BACKUP`/`RESTORE`. We must be able to read the existing RMV (even though it may be invalid).
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none <<EOF
BACKUP DATABASE ${db} TO Memory('bk') FORMAT Null;
DROP DATABASE ${db} SYNC;
RESTORE DATABASE ${db} FROM Memory('bk') FORMAT Null;
EOF

${CLICKHOUSE_CLIENT} -q "SELECT 'restored', name FROM system.tables WHERE database = '${db}' AND name = 'bad_mv'"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP DATABASE ${db} SYNC"
