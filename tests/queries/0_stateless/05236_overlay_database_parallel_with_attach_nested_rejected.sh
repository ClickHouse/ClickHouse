#!/usr/bin/env bash

# An explicit `ATTACH DATABASE ... ENGINE = Overlay(...)` must not persist a facade layered over
# another read-only `Overlay` facade (see `04757_overlay_database_attach_nested_rejected`): the
# source would then be dropped from the union on every lookup. Loading previously-written metadata
# at server startup uses `ATTACH` too and must *not* refuse the definition -- a server that does not
# start is far worse -- so the two cases have to be told apart, and the discriminator is the loader's
# own replay flag rather than `internal`: wrappers such as `PARALLEL WITH` execute user statements
# as internal ones. This test pins that a nesting written through the `PARALLEL WITH` form is
# refused like the plain one, so neither that discriminator nor the `user_initiated` propagation
# the wrapper relies on can change without being noticed.
#
# The databases are named after `CLICKHOUSE_DATABASE` because they are server-wide objects.
# Related: https://github.com/ClickHouse/ClickHouse/pull/86768

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_MID="db_mid_${CLICKHOUSE_DATABASE}"
DB_TOP="db_top_${CLICKHOUSE_DATABASE}"
DB_FLAT="db_flat_${CLICKHOUSE_DATABASE}"
DB_PROBE="db_probe_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_TOP};
DROP DATABASE IF EXISTS ${DB_FLAT};
DROP DATABASE IF EXISTS ${DB_MID};
DROP DATABASE IF EXISTS ${DB_SRC};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_SRC}.t VALUES (1), (2);

CREATE DATABASE ${DB_MID} ENGINE = Overlay('${DB_SRC}');
"

echo "-- PARALLEL WITH ATTACH of a facade over another facade is rejected as well"
$CLICKHOUSE_CLIENT -q "
ATTACH DATABASE ${DB_TOP} ENGINE = Overlay('${DB_MID}')
PARALLEL WITH
ATTACH DATABASE ${DB_FLAT} ENGINE = Overlay('${DB_SRC}')" 2>&1 \
    | grep -o "BAD_ARGUMENTS" | head -1

echo "-- the rejected facade was not persisted"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.databases WHERE name = '${DB_TOP}'"

echo "-- PARALLEL WITH CREATE of a facade over another facade is rejected too"
$CLICKHOUSE_CLIENT -q "
CREATE DATABASE ${DB_TOP} ENGINE = Overlay('${DB_MID}')
PARALLEL WITH
DROP DATABASE IF EXISTS ${DB_PROBE}" 2>&1 \
    | grep -o "BAD_ARGUMENTS" | head -1

echo "-- a facade over a regular database still attaches through PARALLEL WITH, and reads its source"
$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_FLAT};
ATTACH DATABASE ${DB_FLAT} ENGINE = Overlay('${DB_SRC}')
PARALLEL WITH
DROP DATABASE IF EXISTS ${DB_PROBE};
SELECT count() FROM ${DB_FLAT}.t;
"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_TOP};
DROP DATABASE ${DB_FLAT};
DROP DATABASE ${DB_MID};
DROP DATABASE ${DB_SRC};
"
