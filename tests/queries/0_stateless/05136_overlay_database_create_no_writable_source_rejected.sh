#!/usr/bin/env bash
# A read-only `Overlay` facade delegates `CREATE TABLE` to its first writable source database. When
# every source is read-only there is no such database, and the query must be rejected BEFORE the
# facade-wide existence probe: that probe walks the sources, so its answer (`TABLE_ALREADY_EXISTS`,
# or a silent `IF NOT EXISTS`) would tell the caller which names the hidden sources hold, and the
# source-side grant check has nothing to check against. Every `CREATE TABLE` through such a facade
# must fail identically, whatever the name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
printf '"id"\n1\n' > "${DIR}"/taken.csv

DB_FS="db_fs_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_FS};

-- A Filesystem database is read-only, so a facade over it alone has no writable source.
CREATE DATABASE ${DB_FS} ENGINE = Filesystem;
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_FS}');
"

# The file exists, so the name resolves through the facade; the other one does not.
TAKEN="${CLICKHOUSE_TEST_UNIQUE_NAME}/taken.csv"
FREE="${CLICKHOUSE_TEST_UNIQUE_NAME}/free.csv"

rejected()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 \
        | grep -oE 'TABLE_IS_PERMANENTLY_READ_ONLY|TABLE_ALREADY_EXISTS|without a writable source database' \
        | sort -u | tr '\n' ' '
    echo
}

echo 'a taken and a free name are rejected identically'
rejected "CREATE TABLE ${DB_OVL}.\`${TAKEN}\` (id UInt64) ENGINE = MergeTree ORDER BY id"
rejected "CREATE TABLE IF NOT EXISTS ${DB_OVL}.\`${TAKEN}\` (id UInt64) ENGINE = MergeTree ORDER BY id"
rejected "CREATE TABLE ${DB_OVL}.\`${FREE}\` (id UInt64) ENGINE = MergeTree ORDER BY id"
rejected "CREATE TABLE IF NOT EXISTS ${DB_OVL}.\`${FREE}\` (id UInt64) ENGINE = MergeTree ORDER BY id"

echo -n 'the rejection does not name a source: '
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB_OVL}.\`${TAKEN}\` (id UInt64) ENGINE = MergeTree ORDER BY id" 2>&1 | grep -c "${DB_FS}"

echo -n 'nothing was created: '
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = 'db_fs_${CLICKHOUSE_DATABASE}' OR database = 'db_ovl_${CLICKHOUSE_DATABASE}'"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_FS};
"

rm -rf "${DIR}"
