#!/usr/bin/env bash
# A read-only `Overlay` facade has no UUID of its own. `DatabaseCatalog::attachDatabase` asks the
# database for its UUID while holding the catalog lock, so the facade must answer without resolving
# its sources through the catalog (doing so wedged the whole server on the first
# `CREATE DATABASE ... ENGINE = Overlay`), and it must not be registered under a source's UUID.
# Likewise a facade-qualified table id must not carry the UUID of the source table, or the catalog
# would resolve it by UUID straight to the source and fail with `TABLE_UUID_MISMATCH`.
# A table created through the facade is created in the first writable source under that source's
# name, gets a UUID when that source is `Atomic`, and requires the `CREATE TABLE` grant on both the
# facade and the source (the denial names only the facade).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
USER_OVL="u_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP USER IF EXISTS ${USER_OVL};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

SELECT 'the facade has no UUID, the source keeps its own';
SELECT if(name = '${DB_SRC}', 'source', 'facade') AS which, toString(uuid) = '00000000-0000-0000-0000-000000000000' AS nil_uuid
FROM system.databases WHERE name IN ('${DB_SRC}', '${DB_OVL}') ORDER BY which;

SELECT 'a table created through the facade lands in the Atomic source with a UUID';
CREATE TABLE ${DB_OVL}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
SELECT database = '${DB_SRC}', toString(uuid) != '00000000-0000-0000-0000-000000000000' FROM system.tables WHERE database = '${DB_SRC}' AND name = 't';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' ORDER BY name;

SELECT 'the table is reachable through the facade by name, and the facade id carries no UUID';
INSERT INTO ${DB_OVL}.t VALUES (1), (2);
SELECT count() FROM ${DB_OVL}.t;
DESCRIBE TABLE ${DB_OVL}.t;

SELECT 'a name that already resolves through the facade is not created again';
CREATE TABLE IF NOT EXISTS ${DB_OVL}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
SELECT count() FROM system.tables WHERE database = '${DB_SRC}';
"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB_OVL}.t (x UInt8) ENGINE = MergeTree ORDER BY x" 2>&1 | grep -o 'TABLE_ALREADY_EXISTS' | sort -u

echo 'the CREATE TABLE grant is required on the facade and on the source'
$CLICKHOUSE_CLIENT -m -q "
CREATE USER ${USER_OVL} NOT IDENTIFIED;
GRANT CREATE TABLE ON ${DB_OVL}.* TO ${USER_OVL};
GRANT TABLE ENGINE ON MergeTree TO ${USER_OVL};
GRANT SHOW TABLES ON ${DB_OVL}.* TO ${USER_OVL};
GRANT SHOW TABLES ON ${DB_SRC}.* TO ${USER_OVL};
"
$CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "CREATE TABLE ${DB_OVL}.t_by_user (x UInt8) ENGINE = MergeTree ORDER BY x" 2>&1 \
    | grep -o 'ACCESS_DENIED\|underlying source database of this Overlay facade' | sort -u
echo -n 'the denial does not name the source: '
$CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "CREATE TABLE ${DB_OVL}.t_by_user (x UInt8) ENGINE = MergeTree ORDER BY x" 2>&1 | grep -c "${DB_SRC}"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${DB_SRC}' AND name = 't_by_user'"

$CLICKHOUSE_CLIENT -q "GRANT CREATE TABLE ON ${DB_SRC}.* TO ${USER_OVL}"
$CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "CREATE TABLE ${DB_OVL}.t_by_user (x UInt8) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${DB_SRC}' ORDER BY name"

$CLICKHOUSE_CLIENT -m -q "
SELECT 'the facade can be detached and attached again';
DETACH DATABASE ${DB_OVL};
ATTACH DATABASE ${DB_OVL};
SELECT name FROM system.tables WHERE database = '${DB_OVL}' ORDER BY name;

DROP USER ${USER_OVL};
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"
