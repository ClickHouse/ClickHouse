#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: depends on libpq (PostgreSQL database engine), which is not built in fast test.
#
# A server-side (read-only) `Overlay` reports isRemoteDatabase() == true, so it is excluded from the
# default catalog enumeration getDatabases({.with_remote_databases = false}) used by `system.tables`,
# `system.columns` and the asynchronous metrics. This prevents an `Overlay` over a remote source
# (MySQL/PostgreSQL/DataLake) from issuing implicit calls to the remote service during routine
# enumeration, while explicit `SHOW TABLES`, `system.databases` and direct queries still work.
# The `clickhouse-local` (non-read-only) `Overlay` stays non-remote, so the local default database
# keeps showing its tables in `system.tables`.

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

echo 'A read-only Overlay is hidden from system.tables by default (no implicit remote call)'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${OV_LOCAL}'"

echo '... but it is visible with show_remote_databases_in_system_tables = 1'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${OV_LOCAL}' SETTINGS show_remote_databases_in_system_tables = 1"

echo 'An Overlay over an unreachable remote source is also skipped by default (returns quickly, count 0)'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${OV_REMOTE}'"

echo 'SHOW TABLES on the facade still lists everything'
${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${OV_LOCAL}"

echo 'A direct query through the facade works'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${OV_LOCAL}.t"

echo 'system.databases lists the facade regardless of the setting'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${OV_LOCAL}'"

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
