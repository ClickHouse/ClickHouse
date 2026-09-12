#!/usr/bin/env bash
# `CREATE TABLE` through a read-only `Overlay` facade is delegated to its first writable source and
# requires the `CREATE TABLE` grant on both the facade and that source. The source-side grant must be
# proved BEFORE the facade-wide existence probe: otherwise a user with the facade grant alone could
# tell which names the hidden sources hold (`TABLE_ALREADY_EXISTS` or a silent `IF NOT EXISTS`
# for a taken name versus `ACCESS_DENIED` for a free one). Without the source-side grant every
# `CREATE TABLE` through the facade must be denied identically, whatever the name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_HIDDEN="db_hidden_${CLICKHOUSE_DATABASE}"
USER_OVL="u_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP DATABASE IF EXISTS ${DB_HIDDEN};
DROP USER IF EXISTS ${USER_OVL};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE DATABASE ${DB_HIDDEN} ENGINE = Atomic;
CREATE TABLE ${DB_HIDDEN}.secret (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}', '${DB_HIDDEN}');

CREATE USER ${USER_OVL} NOT IDENTIFIED;
GRANT CREATE TABLE ON ${DB_OVL}.* TO ${USER_OVL};
GRANT SHOW TABLES ON ${DB_OVL}.* TO ${USER_OVL};
GRANT TABLE ENGINE ON MergeTree TO ${USER_OVL};
"

denied()
{
    $CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "$1" 2>&1 | sed "s/${CLICKHOUSE_DATABASE}/DB/g" \
        | grep -oE 'Code: [0-9]+|ACCESS_DENIED|TABLE_ALREADY_EXISTS|grant CREATE TABLE ON [^ ]+ in the underlying source database of this Overlay facade' | sort -u | tr '\n' ' '
    echo
}

echo 'without the source-side grant, a taken hidden name and a free name are denied identically'
denied "CREATE TABLE ${DB_OVL}.secret (x UInt8) ENGINE = MergeTree ORDER BY x"
denied "CREATE TABLE IF NOT EXISTS ${DB_OVL}.secret (x UInt8) ENGINE = MergeTree ORDER BY x"
denied "CREATE TABLE ${DB_OVL}.absent (x UInt8) ENGINE = MergeTree ORDER BY x"
denied "CREATE TABLE IF NOT EXISTS ${DB_OVL}.absent (x UInt8) ENGINE = MergeTree ORDER BY x"

echo -n 'the denial does not name a source: '
$CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "CREATE TABLE ${DB_OVL}.secret (x UInt8) ENGINE = MergeTree ORDER BY x" 2>&1 | grep -c "${DB_SRC}\|${DB_HIDDEN}"

echo -n 'nothing was created: '
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database IN ('${DB_SRC}', '${DB_HIDDEN}')"

echo 'with the grant on the receiving source, the facade-wide existence check applies'
$CLICKHOUSE_CLIENT -q "GRANT CREATE TABLE ON ${DB_SRC}.* TO ${USER_OVL}"
denied "CREATE TABLE ${DB_OVL}.secret (x UInt8) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "CREATE TABLE IF NOT EXISTS ${DB_OVL}.secret (x UInt8) ENGINE = MergeTree ORDER BY x" && echo 'IF NOT EXISTS on a taken name is a no-op'
$CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "CREATE TABLE ${DB_OVL}.absent (x UInt8) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "SELECT database = '${DB_SRC}' AS in_first_writable_source, name FROM system.tables WHERE database IN ('${DB_SRC}', '${DB_HIDDEN}') ORDER BY name"

$CLICKHOUSE_CLIENT -m -q "
DROP USER ${USER_OVL};
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
DROP DATABASE ${DB_HIDDEN};
"
