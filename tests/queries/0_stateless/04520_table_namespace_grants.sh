#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE
user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.t\` (x UInt8) ENGINE = Memory;
    CREATE TABLE \`ns.sub.t2\` (x UInt8) ENGINE = Memory;
    CREATE TABLE \`other.t3\` (x UInt8) ENGINE = Memory;
    CREATE TABLE plain (x UInt8) ENGINE = Memory;
    DROP USER IF EXISTS $user;
    CREATE USER $user NOT IDENTIFIED;
"

echo "-- GRANT ON * under a namespace covers the namespace recursively and nothing else"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; GRANT SELECT ON * TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM $db.\`ns.t\`"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM $db.\`ns.sub.t2\`"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM $db.\`other.t3\`" 2>&1 | grep -m1 -c "ACCESS_DENIED"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT count() FROM $db.plain" 2>&1 | grep -m1 -c "ACCESS_DENIED"

echo "-- the stored grant shows the namespace scope"
$CLICKHOUSE_CLIENT -q "SHOW GRANTS FOR $user" | grep -m1 -c "ns."

echo "-- an exact row policy under a namespace targets the namespaced table"
$CLICKHOUSE_CLIENT -m -q "
    USE $db.ns;
    CREATE ROW POLICY rp_$db ON t USING 0 TO $user;
    SELECT database = '$db' AND table = 'ns.t' FROM system.row_policies WHERE short_name = 'rp_$db';
    DROP ROW POLICY rp_$db ON t;
"

echo "-- wildcard row policy operations are rejected under a namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; CREATE ROW POLICY pol_$db ON * USING 1 TO ALL" 2>&1 | grep -m1 -c "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; DROP ROW POLICY IF EXISTS pol_$db ON *" 2>&1 | grep -m1 -c "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SHOW ROW POLICIES ON *" 2>&1 | grep -m1 -c "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SHOW CREATE ROW POLICIES ON *" 2>&1 | grep -m1 -c "BAD_ARGUMENTS"

echo "-- CREATE ON CLUSTER authorizes exactly the created table"
$CLICKHOUSE_CLIENT -m -q "
    GRANT SHOW DATABASES ON $db.* TO $user;
    GRANT CLUSTER ON *.* TO $user;
    GRANT TABLE ENGINE ON Memory TO $user;
    GRANT CREATE TABLE ON $db.\`ns.made_on_cluster\` TO $user;
"
$CLICKHOUSE_CLIENT --user "$user" -m -q "
    SET distributed_ddl_output_mode = 'none';
    SET distributed_ddl_entry_format_version = 2;
    USE $db.ns;
    CREATE TABLE made_on_cluster ON CLUSTER test_shard_localhost (x Int8) ENGINE = Memory;
"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE $db.\`ns.made_on_cluster\`"

echo "-- a quoted component with a literal dot cannot be granted as a path"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $db.\`a.b\`.t TO $user" 2>&1 | grep -m1 -c "Syntax error"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
