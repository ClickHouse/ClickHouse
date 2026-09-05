#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hierarchical names (`db.ns.t` for the table `ns.t` of the database `db`; see 05077_hierarchical_names) in
# `SHOW COLUMNS`, `SHOW INDEXES`, `joinGet`, `dictGet`, qualified asterisks, and the names that are not hierarchical.

db=$CLICKHOUSE_DATABASE

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${db}/db/g"
}

run "CREATE TABLE ${db}.\"ns.t\" (id UInt64, s String, INDEX idx_s s TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY id"
run "INSERT INTO ${db}.ns.t VALUES (1, 'a')"

echo '--- SHOW COLUMNS and SHOW INDEXES'
run "SHOW COLUMNS FROM ${db}.ns.t"
run "SHOW COLUMNS FROM ns.t"
run "SHOW COLUMNS FROM t FROM ${db}.ns"
$CLICKHOUSE_CLIENT -m -q "USE ${db}.ns; SHOW COLUMNS FROM t"
run "SHOW INDEXES FROM ${db}.ns.t"
run "SHOW INDEX FROM t FROM ${db}.ns"
run "SHOW COLUMNS FROM ${db}.ns.nothing"

echo '--- joinGet'
run "CREATE TABLE ${db}.\"ns.j\" (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)"
run "INSERT INTO ${db}.ns.j VALUES (1, 'one')"
run "SELECT joinGet(${db}.ns.j, 'v', toUInt64(1))"
run "SELECT joinGet('${db}.ns.j', 'v', toUInt64(1))"
run "SELECT joinGet(ns.j, 'v', toUInt64(1))"
run "SELECT joinGet(${db}.ns.j, 'v', toUInt64(1)) SETTINGS enable_analyzer = 0"
$CLICKHOUSE_CLIENT -m -q "USE ${db}.ns; SELECT joinGet(j, 'v', toUInt64(1))"

echo '--- dictGet'
run "CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = Memory"
run "INSERT INTO ${db}.src VALUES (1, 'uno')"
run "CREATE DICTIONARY ${db}.ns.d (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src')) LAYOUT(FLAT()) LIFETIME(0)"
run "SHOW DICTIONARIES FROM ${db}"
run "SELECT dictGet(${db}.ns.d, 'v', 1)"
run "SELECT dictGet('${db}.ns.d', 'v', 1)"
run "SELECT dictGet(ns.d, 'v', 1)"
run "SELECT dictGet(${db}.ns.d, 'v', 1) SETTINGS enable_analyzer = 0"
$CLICKHOUSE_CLIENT -m -q "USE ${db}.ns; SELECT dictGet(d, 'v', 1)"
run "EXISTS DICTIONARY ${db}.ns.d"

echo '--- qualified asterisks'
run "SELECT ${db}.ns.t.* FROM ${db}.ns.t"
run "SELECT ns.t.* FROM ns.t"
run "SELECT t.* FROM ns.t AS t"
$CLICKHOUSE_CLIENT -m -q "USE ${db}.ns; SELECT t.* FROM t"

echo '--- a name with an empty part is not hierarchical'
run "CREATE DATABASE \".${db}.\""
run "CREATE TABLE \".${db}.\".\".inner.t\" (x UInt8) ENGINE = Memory"
run "INSERT INTO \".${db}.\".\".inner.t\" VALUES (1)"
run "SELECT * FROM \".${db}.\".\".inner.t\""
run "EXISTS TABLE \".${db}.\".\".inner.t\""
run "SHOW TABLES FROM \".${db}.\""
run "SHOW COLUMNS FROM \".${db}.\".\".inner.t\""
$CLICKHOUSE_CLIENT -m -q "USE \".${db}.\"; SELECT * FROM \".inner.t\"; SHOW TABLES;"
run "DROP DATABASE \".${db}.\""

echo '--- ON CLUSTER: the database may exist on the other hosts only'
run "CREATE TABLE ${db}_nonexistent.t ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory" | grep -o 'UNKNOWN_DATABASE' | sort -u
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "CREATE TABLE ${db}.ns.oc ON CLUSTER test_shard_localhost (x UInt8) ENGINE = Memory"
run "EXISTS TABLE ${db}.\"ns.oc\""
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "RENAME TABLE ${db}.ns.oc TO ${db}.ns.oc2 ON CLUSTER test_shard_localhost"
run "EXISTS TABLE ${db}.\"ns.oc2\""
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "DROP TABLE ${db}.ns.oc2 ON CLUSTER test_shard_localhost"
run "SHOW TABLES FROM ${db}"
