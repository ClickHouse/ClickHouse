#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-fasttest
# Tag no-replicated-database: the test creates its own Replicated database and forces it through replica recovery.
# Tag no-fasttest: needs Keeper.

# Replica recovery of a Replicated database keeps the data of KeeperMap tables whose definition in Keeper differs
# from the local one: altered there, renamed there, or dropped there (also while another replica of the table is still
# registered). A table dropped there while another table uses the same Keeper path is dropped, and that table keeps
# the data.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_rdb"
ZK_PATH="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb"
CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none"
# The queries that rewrite Keeper nodes must run as written (the stress profile fuzzes every query).
ZK_WRITER="${CLICKHOUSE_CLIENT} --ast_fuzzer_runs=0 --ast_fuzzer_any_query=0"

function print_copy()
{
    local copy
    copy=$(${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${DB}_broken_replicated_tables' AND startsWith(name, '$1_')")
    if [ -n "${copy}" ]; then
        ${CLICKHOUSE_CLIENT} -q "SELECT '$1 copy', count() FROM ${DB}_broken_replicated_tables.${copy}"
    else
        echo "$1 copy: none"
    fi
}

function count_recoveries()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.text_log
        WHERE logger_name = 'DatabaseReplicated (${DB})' AND message = 'All tables are created successfully'
        SETTINGS max_rows_to_read = 0"
}

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_tables SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_replicated_tables SYNC"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS km_twin SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"

for t in km_altered km_renamed km_dropped km_replica km_shared; do
    ${CLIENT} -q "CREATE TABLE ${DB}.$t (k UInt64, v String) ENGINE = KeeperMap('/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/$t') PRIMARY KEY k"
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${DB}.$t SELECT number, toString(number) FROM numbers(50)"
    ${CLICKHOUSE_CLIENT} -q "SELECT '$t before', count() FROM ${DB}.$t"
done
# Another table on the Keeper path of km_shared, like the one another replica re-creates there after dropping km_shared.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE km_twin (k UInt64, v String) ENGINE = KeeperMap('/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/km_shared') PRIMARY KEY k"
# The registration of km_replica by a lagging replica on another server: the table UUID followed by that server's UUID.
KM_REPLICA_TABLES="/test_keeper_map/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/km_replica/metadata/tables"
KM_REPLICA_REGISTRATION="$(${CLICKHOUSE_CLIENT} -q "SELECT uuid FROM system.tables WHERE database = '${DB}' AND name = 'km_replica'")00000000-0000-0000-0000-000000000001"
${ZK_WRITER} -q "INSERT INTO system.zookeeper (path, name, value) VALUES ('${KM_REPLICA_TABLES}', '${KM_REPLICA_REGISTRATION}', '')"

# What the other replicas would have after an ALTER, a RENAME and the DROPs this replica missed.
${ZK_WRITER} -q "
    INSERT INTO system.zookeeper (path, name, value)
    SELECT path, name, concat(trimRight(value), '\nCOMMENT \'altered\'\n') FROM system.zookeeper
    WHERE path = '${ZK_PATH}/metadata' AND name = 'km_altered'"
${ZK_WRITER} -q "
    INSERT INTO system.zookeeper (path, name, value)
    SELECT path, 'km_renamed2', value FROM system.zookeeper
    WHERE path = '${ZK_PATH}/metadata' AND name = 'km_renamed'"
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${ZK_PATH}/metadata/km_renamed'"
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${ZK_PATH}/metadata/km_dropped'"
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${ZK_PATH}/metadata/km_replica'"
${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${ZK_PATH}/metadata/km_shared'"

RECOVERIES_BEFORE=$(count_recoveries)
# The digest 42 makes the replica recover itself when the database is attached.
${CLICKHOUSE_KEEPER_CLIENT} -q "set '${ZK_PATH}/replicas/s1|r1/digest' '42'"
${CLICKHOUSE_CLIENT} -q "DETACH DATABASE ${DB}"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${DB}"
for _ in {1..240}; do
    [ "$(count_recoveries)" -gt "${RECOVERIES_BEFORE}" ] && break
    sleep 0.5
done

${CLICKHOUSE_CLIENT} -q "SELECT 'recovered', $(count_recoveries) > ${RECOVERIES_BEFORE}"
${CLICKHOUSE_CLIENT} -q "SELECT 'km_altered', count() FROM ${DB}.km_altered"
${CLICKHOUSE_CLIENT} -q "SELECT 'km_renamed exists', count() FROM system.tables WHERE database = '${DB}' AND name = 'km_renamed'"
${CLICKHOUSE_CLIENT} -q "SELECT 'km_renamed2', count() FROM ${DB}.km_renamed2"
${CLICKHOUSE_CLIENT} -q "SELECT 'km_dropped exists', count() FROM system.tables WHERE database = '${DB}' AND name = 'km_dropped'"
print_copy km_dropped
print_copy km_replica
print_copy km_shared
${CLICKHOUSE_CLIENT} -q "SELECT 'km_twin', count() FROM km_twin"

${CLICKHOUSE_KEEPER_CLIENT} -q "rm '${KM_REPLICA_TABLES}/${KM_REPLICA_REGISTRATION}'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_tables SYNC"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}_broken_replicated_tables SYNC"
