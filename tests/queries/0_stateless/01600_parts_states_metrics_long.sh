#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
verify_sql="SELECT
    (SELECT sumIf(value, metric = 'PartsActive'), sumIf(value, metric = 'PartsOutdated') FROM system.metrics)
    = (SELECT sum(active), sum(NOT active) FROM system.parts)"

# The query is not atomic - it can compare states between system.parts and system.metrics from different points in time.
# So, there is inherent race condition. But it should get expected result eventually.
# In case of test failure, this code will do infinite loop and timeout.
verify()
{
    for i in $(seq 1 3001)
    do
        result=$( $CLICKHOUSE_CLIENT -m --query="$verify_sql" )
        [ "$result" = "1" ] && break

        if [ "$i" = "3000" ]; then
            echo "======="
            $CLICKHOUSE_CLIENT --query="SELECT * FROM system.parts format TSVWithNames"
            echo "======="
            $CLICKHOUSE_CLIENT --query="SELECT * FROM system.metrics format TSVWithNames"
            echo "======="
            return
        fi

        sleep 0.1
    done
    echo 1
}

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP TABLE IF EXISTS test_table"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_table(data Date) ENGINE = MergeTree  PARTITION BY toYear(data) ORDER BY data;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-01')"
verify

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-02')"
verify

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE test_table FINAL"
verify

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP TABLE test_table"
verify
