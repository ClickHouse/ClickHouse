#!/usr/bin/env bash

# A read-only `Overlay` facade cannot use another one as a source, and that is rejected from both
# sides. The direction covered here is the one that cannot be seen when the facade is created:
# `db_top = Overlay('db_mid')` exists already, and `db_mid` is only afterwards (re-)created as a
# facade itself. Rejecting it keeps every persisted facade definition usable, and keeps a facade
# from silently losing a source. `db_mid` may still be re-created as an ordinary database.
#
# The regression guard at the end matters for the whole server: `Overlay` sources are resolved on
# every lookup, and that resolution is reached from whole-server scans (`system.mutations`,
# `system.rocksdb`, the asynchronous metrics, ...) which walk every database. A facade with a source
# it cannot use must therefore never fail such a scan - otherwise one misconfigured database breaks
# queries that have nothing to do with it, for every user.
#
# Related: https://github.com/ClickHouse/ClickHouse/pull/86768

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SUF="${CLICKHOUSE_TEST_UNIQUE_NAME}"
DB_SRC="db_src_${SUF}"
DB_MID="db_mid_${SUF}"
DB_TOP="db_top_${SUF}"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_TOP};
    DROP DATABASE IF EXISTS ${DB_MID};
    DROP DATABASE IF EXISTS ${DB_SRC};

    CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
    CREATE TABLE ${DB_SRC}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_SRC}.t VALUES (1), (2);

    CREATE DATABASE ${DB_MID} ENGINE = Atomic;
    CREATE TABLE ${DB_MID}.m (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_MID}.m VALUES (3);

    CREATE DATABASE ${DB_TOP} ENGINE = Overlay('${DB_MID}');
"

echo 'The facade reads its source'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB_TOP}.m"

echo 'A facade cannot be created over a name that another facade uses as a source'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_MID}"
# The client prints the error text more than once, so match it with `grep -q` instead of counting.
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_MID} ENGINE = Overlay('${DB_SRC}')" 2>&1 |
    grep -q 'BAD_ARGUMENTS' && echo 'rejected'

echo 'The same holds for ATTACH'
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB_MID} ENGINE = Overlay('${DB_SRC}')" 2>&1 |
    grep -q 'BAD_ARGUMENTS' && echo 'rejected'

echo 'Re-creating the source as an ordinary database is allowed, and the facade sees it again'
${CLICKHOUSE_CLIENT} -nm --query "
    CREATE DATABASE ${DB_MID} ENGINE = Atomic;
    CREATE TABLE ${DB_MID}.m (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${DB_MID}.m VALUES (3), (4);
    SELECT count() FROM ${DB_TOP}.m;
"

echo 'Whole-server scans work while a facade has a source it cannot use'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_MID}"
${CLICKHOUSE_CLIENT} -nm --query "
    SELECT count() >= 0 FROM system.mutations;
    SELECT count() >= 0 FROM system.tables WHERE database = '${DB_TOP}';
"

${CLICKHOUSE_CLIENT} -nm --query "
    DROP DATABASE IF EXISTS ${DB_TOP};
    DROP DATABASE IF EXISTS ${DB_SRC};
"
