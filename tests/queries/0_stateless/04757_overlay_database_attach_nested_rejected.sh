#!/usr/bin/env bash
# An explicit `ATTACH DATABASE ... ENGINE = Overlay(...)` is user-facing DDL just like `CREATE`,
# so it must not persist a facade layered over another read-only `Overlay` facade: such a database
# would fail every later lookup in `resolveDatabases` with `BAD_ARGUMENTS`. `CREATE` already
# rejects the nesting up front; this test pins the same rejection for the explicit `ATTACH` form,
# and checks that a legitimate `DETACH DATABASE` / `ATTACH DATABASE` roundtrip of a valid facade
# keeps working.
#
# The databases are named after `CLICKHOUSE_DATABASE` because they are server-wide objects: with
# fixed names, another run of this same test (the flaky check runs it repeatedly) drops the
# databases from under this one.
# Related: https://github.com/ClickHouse/ClickHouse/pull/86768

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_MID="db_mid_${CLICKHOUSE_DATABASE}"
DB_TOP="db_top_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_TOP};
DROP DATABASE IF EXISTS ${DB_MID};
DROP DATABASE IF EXISTS ${DB_SRC};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_SRC}.t VALUES (1), (2);

CREATE DATABASE ${DB_MID} ENGINE = Overlay('${DB_SRC}');
"

echo "-- ATTACH of a facade over another facade is rejected"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB_TOP} ENGINE = Overlay('${DB_MID}')" 2>&1 \
    | grep -o "BAD_ARGUMENTS" | head -1

echo "-- listing a facade among several sources is rejected too"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${DB_TOP} ENGINE = Overlay('${DB_SRC}', '${DB_MID}')" 2>&1 \
    | grep -o "BAD_ARGUMENTS" | head -1

echo "-- the rejected facade was not persisted"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB_TOP}'"

echo "-- ATTACH of a facade over a regular database works"
$CLICKHOUSE_CLIENT -m -q "
ATTACH DATABASE ${DB_TOP} ENGINE = Overlay('${DB_SRC}');
SELECT count() FROM ${DB_TOP}.t;
"

echo "-- DETACH / ATTACH roundtrip of a valid facade keeps working"
$CLICKHOUSE_CLIENT -m -q "
DETACH DATABASE ${DB_MID};
ATTACH DATABASE ${DB_MID};
SELECT count() FROM ${DB_MID}.t;
"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE ${DB_TOP};
DROP DATABASE ${DB_MID};
DROP DATABASE ${DB_SRC};
"
