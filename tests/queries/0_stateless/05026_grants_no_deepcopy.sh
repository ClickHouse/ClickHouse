#!/usr/bin/env bash
# Tags: no-parallel, no-sanitizers
# Regression test: GRANT / SHOW GRANTS / REVOKE keep working as expected
# when a user holds many per-table grants. This is a functional regression
# test covering the change in getGrantQueriesImpl to avoid deep-copying the
# AccessRights tree when it is only read (the copy was O(number of nodes) and
# could be a bottleneck for users with a huge number of per-table grants).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_USER=test_grants_no_deepcopy
TEST_DB=test_db_grants_no_deepcopy

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $TEST_USER"
$CLICKHOUSE_CLIENT -q "CREATE USER $TEST_USER"
$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $TEST_DB"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE $TEST_DB"

# Create 30 real tables and grant a privilege on each of them,
# so that SHOW GRANTS has to walk a larger AccessRights tree.
for i in $(seq 1 30); do
    case $((i % 3)) in
        1) PRIV=SELECT ;;
        2) PRIV=INSERT ;;
        0) PRIV=ALTER ;;
    esac
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $TEST_DB.t$i (a UInt64) ENGINE = Memory"
    $CLICKHOUSE_CLIENT -q "GRANT $PRIV ON $TEST_DB.t$i TO $TEST_USER"
done

# SHOW GRANTS must return one row per granted table.
$CLICKHOUSE_CLIENT -q "SHOW GRANTS FOR $TEST_USER" | awk 'END {print NR}'

# Re-granting the same privilege must be a no-op.
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON $TEST_DB.t1 TO $TEST_USER"
$CLICKHOUSE_CLIENT -q "SHOW GRANTS FOR $TEST_USER" | awk 'END {print NR}'

# Revoking one grant must remove exactly one row.
$CLICKHOUSE_CLIENT -q "REVOKE INSERT ON $TEST_DB.t2 FROM $TEST_USER"
$CLICKHOUSE_CLIENT -q "SHOW GRANTS FOR $TEST_USER" | awk 'END {print NR}'

$CLICKHOUSE_CLIENT -q "DROP USER $TEST_USER"
# Drop all tables first, then the database.
for i in $(seq 1 30); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $TEST_DB.t$i"
done
$CLICKHOUSE_CLIENT -q "DROP DATABASE $TEST_DB"
