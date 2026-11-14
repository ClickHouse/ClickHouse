#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-replicated-database, no-fasttest
# Tag no-parallel: enables a REGULAR failpoint that affects the whole server process.
# Tag no-replicated-database: the test creates its own Replicated databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE rewrites the definitions of the views owned by Replicated databases: in Keeper, in the replica
# digest and in the local metadata. Adapted from 04409 of #110982.

R1="${CLICKHOUSE_DATABASE}_r1"
R2="${CLICKHOUSE_DATABASE}_r2"
EXT="${CLICKHOUSE_DATABASE}_ext"
A="${CLICKHOUSE_DATABASE}_a"
B="${CLICKHOUSE_DATABASE}_b"
U1="${CLICKHOUSE_DATABASE}_u1"
U2="${CLICKHOUSE_DATABASE}_u2"
ZK_PATH="/test/rename_database_view_references/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Every digest assertion runs (no 1/16 sampling) and throws `Digest does not match` on a divergence.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT database_replicated_force_metadata_digest_check"
trap '${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT database_replicated_force_metadata_digest_check"' EXIT

function rows()
{
    echo "$(echo "$1" | sed "s/${CLICKHOUSE_DATABASE}//"): $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $1")"
}

# The views whose definition in Keeper names `$3` and not `$2`.
function keeper_definitions()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT arraySort(groupArray(name)) FROM system.zookeeper
        WHERE path = '$1/metadata' AND position(value, '$2.') = 0 AND position(value, '$3.') > 0"
}

function probe()
{
    ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE TABLE $1.probe$2 (id UInt64) ENGINE = ReplicatedMergeTree ORDER BY id"
    ${CLICKHOUSE_CLIENT} -q "EXISTS TABLE $1.probe$2"
}

echo "-- views of the renamed Replicated database"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${R1} ENGINE = Replicated('${ZK_PATH}/r1', 's1', 'r1')"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE TABLE ${R1}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${R1}.dest (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW ${R1}.mv TO ${R1}.dest AS SELECT x FROM ${R1}.src;
    CREATE MATERIALIZED VIEW ${R1}.inline_mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM ${R1}.src;
    CREATE VIEW ${R1}.pv AS SELECT x FROM ${R1}.src;
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "RENAME DATABASE ${R1} TO ${R2}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${R2}.src SELECT number FROM numbers(3)"
rows "${R2}.dest"
rows "${R2}.inline_mv"
rows "${R2}.pv"
keeper_definitions "${ZK_PATH}/r1" "${R1}" "${R2}"
probe "${R2}" 1

${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${R2}; ATTACH DATABASE ${R2}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${R2}.src SELECT number FROM numbers(3)"
rows "${R2}.dest"
rows "${R2}.inline_mv"
rows "${R2}.pv"
probe "${R2}" 2

echo "-- views of a Replicated database over a renamed Atomic database"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${A} ENGINE = Atomic;
    CREATE TABLE ${A}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE DATABASE ${EXT} ENGINE = Replicated('${ZK_PATH}/ext', 's1', 'r1');
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "
    CREATE VIEW ${EXT}.v AS SELECT * FROM ${A}.t;
    CREATE TABLE ${EXT}.d (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW ${EXT}.mv TO ${EXT}.d AS SELECT * FROM ${A}.t;
"
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${A} TO ${B}"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${B}.t SELECT number FROM numbers(4)"
rows "${EXT}.v"
rows "${EXT}.d"
keeper_definitions "${ZK_PATH}/ext" "${A}" "${B}"
probe "${EXT}" 1

echo "-- a view of the renamed Replicated database over another database"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${U1} ENGINE = Replicated('${ZK_PATH}/u1', 's1', 'r1')"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE VIEW ${U1}.v AS SELECT * FROM ${B}.t"
before=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.zookeeper WHERE path = '${ZK_PATH}/u1/metadata' AND name = 'v'")
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${U1} TO ${U2}"
after=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.zookeeper WHERE path = '${ZK_PATH}/u1/metadata' AND name = 'v'")
[ -n "$before" ] && [ "$before" = "$after" ] && echo "the definition in Keeper is unchanged"
rows "${U2}.v"
probe "${U2}" 1

echo "-- Keeper holds a definition this replica has not applied yet"
C="${CLICKHOUSE_DATABASE}_c"
D="${CLICKHOUSE_DATABASE}_d"
NW="${CLICKHOUSE_DATABASE}_nw"
${CLICKHOUSE_CLIENT} -q "
    CREATE DATABASE ${C} ENGINE = Atomic;
    CREATE TABLE ${C}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE DATABASE ${NW} ENGINE = Replicated('${ZK_PATH}/nw', 's1', 'r1');
"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE VIEW ${NW}.v AS SELECT * FROM ${C}.t"
# Simulates a newer definition that another replica has written to Keeper.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO system.zookeeper (path, name, value)
    SELECT path, name, replaceOne(value, 'SELECT *', 'SELECT x') FROM system.zookeeper
    WHERE path = '${ZK_PATH}/nw/metadata' AND name = 'v'"
newer=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.zookeeper WHERE path = '${ZK_PATH}/nw/metadata' AND name = 'v'")
${CLICKHOUSE_CLIENT} -q "RENAME DATABASE ${C} TO ${D}"
after=$(${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.zookeeper WHERE path = '${ZK_PATH}/nw/metadata' AND name = 'v'")
[ -n "$newer" ] && [ "$newer" = "$after" ] && echo "the newer definition in Keeper is kept"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${D}.t VALUES (1)"
rows "${NW}.v"
probe "${NW}" 1

for db in ${R2} ${EXT} ${B} ${U2} ${D} ${NW}; do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db} SYNC"
done
