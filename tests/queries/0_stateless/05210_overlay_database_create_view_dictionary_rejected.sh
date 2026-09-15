#!/usr/bin/env bash
# Only `CREATE TABLE` is delegated through a read-only `Overlay` facade to its first writable source
# database. `CREATE VIEW`, `CREATE MATERIALIZED VIEW` and `CREATE DICTIONARY` on the facade must be
# rejected with `TABLE_IS_PERMANENTLY_READ_ONLY` instead of being created in the source, in every
# syntax (`IF NOT EXISTS`, `OR REPLACE`) and regardless of whether the name is taken. A materialized
# view that lives in an ordinary database and only writes `TO` a facade name is unaffected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_SRC="db_src_${CLICKHOUSE_DATABASE}"
DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};
CREATE DATABASE ${DB_SRC};
CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');
CREATE TABLE ${DB_SRC}.t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
"

rejected()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | grep -oE 'TABLE_IS_PERMANENTLY_READ_ONLY|Run CREATE (VIEW|DICTIONARY)' | sort -u | tr '\n' ' '
    echo
}

echo 'views and dictionaries on the facade are rejected'
rejected "CREATE VIEW ${DB_OVL}.v AS SELECT id FROM ${DB_SRC}.t"
rejected "CREATE VIEW IF NOT EXISTS ${DB_OVL}.v AS SELECT id FROM ${DB_SRC}.t"
rejected "CREATE OR REPLACE VIEW ${DB_OVL}.v AS SELECT id FROM ${DB_SRC}.t"
rejected "CREATE VIEW ${DB_OVL}.t AS SELECT id FROM ${DB_SRC}.t"
rejected "CREATE MATERIALIZED VIEW ${DB_OVL}.mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${DB_SRC}.t"
rejected "CREATE MATERIALIZED VIEW ${DB_OVL}.mv TO ${DB_SRC}.t AS SELECT id, s FROM ${DB_SRC}.t"
rejected "CREATE DICTIONARY ${DB_OVL}.d (id UInt64, s String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't' DB '${DB_SRC}')) LAYOUT(FLAT()) LIFETIME(0)"
rejected "CREATE DICTIONARY IF NOT EXISTS ${DB_OVL}.d (id UInt64, s String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't' DB '${DB_SRC}')) LAYOUT(FLAT()) LIFETIME(0)"

echo -n 'nothing was created in the source: '
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = '${DB_SRC}' AND name != 't'"

echo 'CREATE TABLE is still delegated, and a materialized view in an ordinary database may write TO the facade'
$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE ${DB_OVL}.t2 (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW ${DB_SRC}.mv TO ${DB_OVL}.t2 AS SELECT id, s FROM ${DB_SRC}.t;
INSERT INTO ${DB_SRC}.t VALUES (1, 'a');
SELECT database, name FROM system.tables WHERE database = '${DB_SRC}' ORDER BY name;
SELECT * FROM ${DB_OVL}.t2;
DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"
