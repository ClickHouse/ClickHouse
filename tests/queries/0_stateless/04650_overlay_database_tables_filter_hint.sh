#!/usr/bin/env bash
# A read-only `Overlay` facade forwards the `system.tables` name filter (the `TablesFilter` hint)
# to each of its source databases, so a source able to push the hint down to an external catalog
# keeps doing so behind the facade, and the names-only listing stays a names-only listing instead
# of falling back to the heavyweight iterator that resolves the storage of every source table.
# This test pins the observable contract of those overrides: whatever the filter shape, the facade
# lists exactly the union of its sources' tables, and a shadowed name appears once.
#
# The databases are named after `CLICKHOUSE_DATABASE` because they are server-wide objects: with
# fixed names, another run of this same test (the flaky check runs it repeatedly) drops the
# databases from under this one.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
DB_SRC_A="db_src_a_${CLICKHOUSE_DATABASE}"
DB_SRC_B="db_src_b_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC_A};
DROP DATABASE IF EXISTS ${DB_SRC_B};

CREATE DATABASE ${DB_SRC_A};
CREATE DATABASE ${DB_SRC_B};

CREATE TABLE ${DB_SRC_A}.pref_one (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB_SRC_A}.pref_two (x UInt64) ENGINE = Memory;
CREATE TABLE ${DB_SRC_A}.other (x UInt64) ENGINE = Memory;
-- The same name in both sources: the first listed source wins, and the facade reports one row.
CREATE TABLE ${DB_SRC_A}.shadowed (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB_SRC_B}.shadowed (y String) ENGINE = Memory;
CREATE TABLE ${DB_SRC_B}.only_in_b (x UInt64) ENGINE = Memory;

CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC_A}', '${DB_SRC_B}');

SELECT 'no filter';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' ORDER BY name;

SELECT 'equality filter, names only';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' AND name = 'pref_one';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' AND name = 'only_in_b';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' AND name = 'no_such_table';

-- A filter on \`engine\` or \`uuid\` needs the source storage, so this takes the heavyweight hint
-- iterator instead of the names-only one. A table listed under the facade reports a nil \`uuid\`,
-- because the facade name is not the identity of the table; the filtered listing must agree with
-- the unfiltered one about that.
SELECT 'equality filter, engine and uuid resolved through the facade';
SELECT name, engine FROM system.tables WHERE database = '${DB_OVL}' AND name = 'pref_one';
SELECT name, uuid FROM system.tables WHERE database = '${DB_OVL}' AND name = 'pref_one';
SELECT name, uuid FROM system.tables WHERE database = '${DB_OVL}' ORDER BY name;

SELECT 'LIKE filter';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' AND name LIKE 'pref\\_%' ORDER BY name;

SELECT 'startsWith filter';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' AND startsWith(name, 'only') ORDER BY name;

SELECT 'a shadowed name is reported once, resolved to the first source';
SELECT name, engine FROM system.tables WHERE database = '${DB_OVL}' AND name = 'shadowed';

SELECT 'SHOW TABLES with a pattern';
SHOW TABLES FROM ${DB_OVL} LIKE 'pref\\_%';

SELECT 'the sources themselves are unaffected';
SELECT name FROM system.tables WHERE database = '${DB_SRC_B}' ORDER BY name;

DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC_A};
DROP DATABASE ${DB_SRC_B};
"
