#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: a `Replicated` database rejects `ON CLUSTER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The statements that take a `[db.]table` target (`OPTIMIZE`, `SYSTEM`, `ALTER`, `DELETE`, the row policies) check the
# access for the database and the table a hierarchical name resolves to (`db.ns.t` is the table `ns.t` of the
# database `db`), not for the name as written: a grant on the nonexistent database `db.ns` allows none of them,
# also when they are dispatched `ON CLUSTER`.

db=$CLICKHOUSE_DATABASE
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
policy="p_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function run()
{
    $CLICKHOUSE_CLIENT -q "$1" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

function denied_as()
{
    $CLICKHOUSE_CLIENT --user "$1" -q "$2" 2>&1 | grep -o 'ACCESS_DENIED' | sort -u
}

function run_as()
{
    $CLICKHOUSE_CLIENT --user "$1" -q "$2" 2>&1 | sed "s/${CLICKHOUSE_TEST_UNIQUE_NAME}/unique/g; s/${db}/db/g"
}

run "CREATE TABLE ${db}.\"ns.t\" (x UInt8) ENGINE = MergeTree ORDER BY x"
run "INSERT INTO ${db}.ns.t VALUES (1), (2)"
run "DROP USER IF EXISTS ${user}"
run "CREATE USER ${user}"
run "GRANT SHOW DATABASES ON ${db}.* TO ${user}"

echo '--- grants on the nonexistent database db.ns only'
run "GRANT OPTIMIZE, SYSTEM MERGES, ALTER, ALTER DELETE, CREATE ROW POLICY, DROP ROW POLICY, SHOW ROW POLICIES ON \"${db}.ns\".* TO ${user}"
denied_as "${user}" "OPTIMIZE TABLE ${db}.ns.t FINAL"
denied_as "${user}" "OPTIMIZE TABLE ${db}.ns.t ON CLUSTER test_shard_localhost FINAL"
denied_as "${user}" "SYSTEM STOP MERGES ${db}.ns.t"
denied_as "${user}" "SYSTEM STOP MERGES ON CLUSTER test_shard_localhost ${db}.ns.t"
denied_as "${user}" "ALTER TABLE ${db}.ns.t ADD COLUMN y UInt8"
denied_as "${user}" "ALTER TABLE ${db}.ns.t ON CLUSTER test_shard_localhost ADD COLUMN y UInt8"
denied_as "${user}" "DELETE FROM ${db}.ns.t WHERE x = 1"
denied_as "${user}" "CREATE ROW POLICY ${policy} ON ${db}.ns.t USING x = 1 TO ALL"
denied_as "${user}" "CREATE ROW POLICY ${policy} ON CLUSTER test_shard_localhost ON ${db}.ns.t USING x = 1 TO ALL"
denied_as "${user}" "DROP ROW POLICY ${policy} ON ${db}.ns.t"
denied_as "${user}" "MOVE ROW POLICY ${policy} ON ${db}.ns.t TO users_xml"
denied_as "${user}" "SHOW CREATE ROW POLICY ${policy} ON ${db}.ns.t"
run "SELECT count() FROM ${db}.ns.t"
run "SELECT count() FROM system.row_policies WHERE database = '${db}'"

echo '--- and with grants on the database db'
run "GRANT OPTIMIZE, SYSTEM MERGES, ALTER, ALTER DELETE, CREATE ROW POLICY, DROP ROW POLICY, SHOW ROW POLICIES ON ${db}.* TO ${user}"
run_as "${user}" "OPTIMIZE TABLE ${db}.ns.t FINAL"
run_as "${user}" "SYSTEM STOP MERGES ${db}.ns.t"
run_as "${user}" "SYSTEM START MERGES ${db}.ns.t"
run_as "${user}" "ALTER TABLE ${db}.ns.t ADD COLUMN y UInt8"
run "DESCRIBE ${db}.ns.t"
run_as "${user}" "DELETE FROM ${db}.ns.t WHERE x = 1"
run "SELECT count() FROM ${db}.ns.t"
run_as "${user}" "CREATE ROW POLICY ${policy} ON ${db}.ns.t USING x = 1 TO ALL"
run "SELECT database, table FROM system.row_policies WHERE database = '${db}'"
run_as "${user}" "SHOW CREATE ROW POLICY ${policy} ON ${db}.ns.t"
run_as "${user}" "DROP ROW POLICY ${policy} ON ${db}.ns.t"
run "SELECT count() FROM system.row_policies WHERE database = '${db}'"

run "DROP USER ${user}"
