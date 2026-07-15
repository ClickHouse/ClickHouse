#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: depends on libpq (PostgreSQL database engine), which is not built in fast test.
#   no-parallel: creates a PostgreSQL database pointing at an unreachable host. Because
#     `show_remote_databases_in_system_tables` defaults to `true`, that database (and the facade
#     over it) is visible in `system.tables` / `system.columns`, so any concurrent query that
#     scans those tables without a database filter would try to connect to the unreachable host
#     and fail with `POSTGRESQL_CONNECTION_FAILURE`.
#
# A server-side (read-only) `Overlay` reports isRemoteDatabase() == true, so it follows
# `show_remote_databases_in_system_tables` exactly like the remote database engines it may wrap:
# visible in `system.tables` by default, excluded when the setting is disabled — in which case
# routine enumeration issues no implicit calls to a remote service behind the facade. It also
# stays excluded from internal consumers that never enumerate remote databases (asynchronous
# metrics). Explicit `SHOW TABLES`, `system.databases` and direct queries through the facade work
# regardless of the setting. The `clickhouse-local` (non-read-only) `Overlay` stays non-remote, so
# the local default database keeps showing its tables in `system.tables`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_LOCAL="db_local_${SUF}"
DB_REMOTE="db_remote_${SUF}"
OV_LOCAL="ov_local_${SUF}"
OV_REMOTE="ov_remote_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OV_LOCAL};
    DROP DATABASE IF EXISTS ${OV_REMOTE};
    DROP DATABASE IF EXISTS ${DB_LOCAL};
    DROP DATABASE IF EXISTS ${DB_REMOTE};

    CREATE DATABASE ${DB_LOCAL} ENGINE = Atomic;
    CREATE TABLE ${DB_LOCAL}.t (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_LOCAL}.t VALUES (1), (2);

    -- The PostgreSQL database engine does not connect at CREATE time, so a non-existent host is fine here.
    CREATE DATABASE ${DB_REMOTE} ENGINE = PostgreSQL('192.0.2.1:5432', 'fake_db', 'user', 'password');

    -- A read-only Overlay over a remote source is allowed (it is no longer rejected).
    CREATE DATABASE ${OV_REMOTE} ENGINE = Overlay('${DB_REMOTE}', '${DB_LOCAL}');

    -- A read-only Overlay over only local sources.
    CREATE DATABASE ${OV_LOCAL} ENGINE = Overlay('${DB_LOCAL}');
"

echo 'A read-only Overlay is visible in system.tables by default, like other remote-flagged databases'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${OV_LOCAL}'"

echo '... and hidden with show_remote_databases_in_system_tables = 0 (no implicit remote call)'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${OV_LOCAL}' SETTINGS show_remote_databases_in_system_tables = 0"

echo 'An Overlay over an unreachable remote source is skipped with the setting disabled (returns quickly, count 0)'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${OV_REMOTE}' SETTINGS show_remote_databases_in_system_tables = 0"

echo 'SHOW TABLES on the facade still lists everything'
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${OV_LOCAL}"

echo 'A direct query through the facade works'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${OV_LOCAL}.t"

echo 'system.databases lists the facade regardless of the setting'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${OV_LOCAL}' SETTINGS show_remote_databases_in_system_tables = 0"

echo 'clickhouse-local: its default Overlay database is NOT remote, so its tables show in system.tables'
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (id UInt32) ENGINE = Memory;
    INSERT INTO t VALUES (1);
    SELECT count() > 0 FROM system.tables WHERE database = currentDatabase() AND name = 't';
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OV_LOCAL};
    DROP DATABASE IF EXISTS ${OV_REMOTE};
    DROP DATABASE IF EXISTS ${DB_LOCAL};
    DROP DATABASE IF EXISTS ${DB_REMOTE};
"
