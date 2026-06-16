#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: depends on libpq (PostgreSQL database engine), which is not built in fast test.
#
# A server-side `Overlay` is enumerated as a regular (non-remote) database by `system.tables`,
# `system.columns` and the asynchronous metrics. Those consumers deliberately skip remote
# databases (MySQL/PostgreSQL/DataLake) unless `show_remote_databases_in_system_tables` is set,
# because enumerating them can issue implicit calls to the remote service. A remote source reached
# through the read-only `Overlay` facade would bypass that protection, so a remote source is
# rejected at CREATE time. This test proves the rejection (regardless of the source position),
# and that an `Overlay` over only local databases is still accepted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"

DB_LOCAL="db_local_${SUF}"
DB_REMOTE="db_remote_${SUF}"
OVL="ovl_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OVL};
    DROP DATABASE IF EXISTS ${DB_LOCAL};
    DROP DATABASE IF EXISTS ${DB_REMOTE};

    CREATE DATABASE ${DB_LOCAL} ENGINE = Atomic;

    -- The PostgreSQL database engine does not connect at CREATE time, so a non-existent host is fine
    -- here: it is enough that the database reports itself as remote (isRemoteDatabase() == true).
    CREATE DATABASE ${DB_REMOTE} ENGINE = PostgreSQL('192.0.2.1:5432', 'fake_db', 'user', 'password');
"

echo 'Overlay over a remote source is rejected (remote listed first)'
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${OVL} ENGINE = Overlay('${DB_REMOTE}', '${DB_LOCAL}');
" 2>&1 | grep -o BAD_ARGUMENTS | uniq

echo 'Overlay over a remote source is rejected (remote listed second)'
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${OVL} ENGINE = Overlay('${DB_LOCAL}', '${DB_REMOTE}');
" 2>&1 | grep -o BAD_ARGUMENTS | uniq

echo 'Overlay over only local sources is accepted'
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${OVL} ENGINE = Overlay('${DB_LOCAL}');
" >/dev/null 2>&1 && echo "CREATE: allowed" || echo "CREATE: denied"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${OVL};
    DROP DATABASE IF EXISTS ${DB_LOCAL};
    DROP DATABASE IF EXISTS ${DB_REMOTE};
"
