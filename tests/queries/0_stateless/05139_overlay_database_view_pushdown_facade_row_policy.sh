#!/usr/bin/env bash
# `optimize_trivial_view_pushdown_to_distributed` must not fire when a row policy applies to a
# trivial view over a `Distributed` table: the policy has to be enforced in the view-output
# namespace by `StorageView::readImpl`. Behind a read-only `Overlay` facade the view is written as
# `facade.v` but resolves to `source.v`, so a policy on the facade name alone is only visible to the
# effective-policy test keyed by the name as written. Asking for the resolved name only made the
# pushdown fire while a non-pushable row-policy filter had already been built, and the query failed
# with `ILLEGAL_PREWHERE` instead of returning the filtered rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
USER_OVL="u_ovl_${CLICKHOUSE_DATABASE}"
POLICY="p_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
DROP USER IF EXISTS ${USER_OVL};

CREATE DATABASE ${DB_SRC} ENGINE = Atomic;
CREATE TABLE ${DB_SRC}.t (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB_SRC}.t VALUES (1), (2), (3);
CREATE TABLE ${DB_SRC}.d (id UInt64) ENGINE = Distributed(test_shard_localhost, '${DB_SRC}', 't');
CREATE VIEW ${DB_SRC}.v AS SELECT * FROM ${DB_SRC}.d;
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

CREATE USER ${USER_OVL} NOT IDENTIFIED;
GRANT SELECT ON ${DB_SRC}.* TO ${USER_OVL};
GRANT SELECT ON ${DB_OVL}.* TO ${USER_OVL};
-- The policy names the facade only.
CREATE ROW POLICY ${POLICY} ON ${DB_OVL}.v USING id = 2 TO ${USER_OVL};
"

for pushdown in 0 1
do
    echo -n "optimize_trivial_view_pushdown_to_distributed = ${pushdown}: "
    $CLICKHOUSE_CLIENT --user "${USER_OVL}" -q "
        SELECT id FROM ${DB_OVL}.v ORDER BY id
        SETTINGS optimize_trivial_view_pushdown_to_distributed = ${pushdown}" 2>&1 \
        | grep -oE '^[0-9]+$|ILLEGAL_PREWHERE|Code: [0-9]+' | tr '\n' ' '
    echo
done

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY ${POLICY} ON ${DB_OVL}.v;
DROP USER ${USER_OVL};
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"
