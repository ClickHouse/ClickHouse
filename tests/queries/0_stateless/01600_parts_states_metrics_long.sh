#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: database = $CLICKHOUSE_DATABASE is unwanted
verify_sql="SELECT
    (SELECT sumIf(value, metric = 'PartsActive'), sumIf(value, metric = 'PartsOutdated') FROM system.metrics)
    = (SELECT sum(active), sum(NOT active) FROM
    (SELECT active FROM system.parts UNION ALL SELECT active FROM system.projection_parts UNION ALL SELECT 1 FROM system.dropped_tables_parts))"

# The query is not atomic - it can compare states between system.parts and system.metrics from different points in time.
# So, there is inherent race condition. But it should get expected result eventually.
# In case of test failure, this code will do infinite loop and timeout.
verify()
{
    for i in {1..5000}
    do
        result=$( $CLICKHOUSE_CLIENT --query="$verify_sql" )
        [ "$result" = "1" ] && echo "$result" && break
        sleep 0.1

        if [[ $i -eq 5000 ]]
        then
            $CLICKHOUSE_CLIENT "
              SELECT sumIf(value, metric = 'PartsActive'), sumIf(value, metric = 'PartsOutdated') FROM system.metrics;
              SELECT sum(active), sum(NOT active) FROM system.parts;
              SELECT sum(active), sum(NOT active) FROM system.projection_parts;
              SELECT count() FROM system.dropped_tables_parts;
            "
        fi
    done
}

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP TABLE IF EXISTS test_table"
$CLICKHOUSE_CLIENT --query="CREATE TABLE test_table (data Date) ENGINE = MergeTree PARTITION BY toYear(data) ORDER BY data;"

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-01')"
verify

$CLICKHOUSE_CLIENT --query="INSERT INTO test_table VALUES ('1992-01-02')"
verify

$CLICKHOUSE_CLIENT --query="OPTIMIZE TABLE test_table FINAL"
verify

$CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=1 --query="DROP TABLE test_table"
verify
