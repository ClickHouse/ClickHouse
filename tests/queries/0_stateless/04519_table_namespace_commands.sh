#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.t\` (x UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`ns.t\` VALUES (1), (2), (3);
"

echo "-- currentDatabase reports the physical database"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SELECT currentDatabase() == '$db'"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SELECT currentSchemas(true) = ['$db']"
$CLICKHOUSE_CLIENT -q "SELECT currentDatabase() == '$db'"

echo "-- system.processes agrees with currentDatabase under a namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SELECT current_database = currentDatabase() FROM system.processes WHERE query_id = queryID()"

echo "-- INSERT under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; INSERT INTO t VALUES (4); SELECT count() FROM t"

echo "-- DESCRIBE under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; DESCRIBE TABLE t" | wc -l

echo "-- lightweight DELETE under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; DELETE FROM t WHERE x = 4; SELECT count() FROM t"

echo "-- RENAME under the namespace stays inside it"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; RENAME TABLE t TO renamed"
$CLICKHOUSE_CLIENT -m -q "EXISTS TABLE $db.\`ns.renamed\`"
$CLICKHOUSE_CLIENT -m -q "EXISTS TABLE $db.renamed"

echo "-- DROP ON CLUSTER ships the namespace-qualified name"
$CLICKHOUSE_CLIENT -m -q "
    SET distributed_ddl_output_mode = 'none';
    USE $db.ns;
    CREATE TABLE cluster_victim (x Int8) ENGINE = Memory;
    DROP TABLE cluster_victim ON CLUSTER test_shard_localhost;
    SELECT count() FROM system.tables WHERE database = '$db' AND name = 'ns.cluster_victim';
"

echo "-- RENAME with a namespace-path source"
$CLICKHOUSE_CLIENT -q "CREATE TABLE \`ns2.src\` (x Int8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ns2.src TO \`ns2.dst\`"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE $db.\`ns2.dst\`"

echo "-- TRUNCATE and DROP under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; TRUNCATE TABLE renamed; SELECT count() FROM renamed"
$CLICKHOUSE_CLIENT -m -q "
    USE $db.ns;
    CREATE TABLE droppable (x Int8) ENGINE = Memory;
    DROP TABLE droppable;
    SELECT count() FROM system.tables WHERE database = '$db' AND name = 'ns.droppable';
"
