#!/usr/bin/env bash
# Tags: long, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function query()
{
    # NOTE: database_atomic_wait_for_drop_and_detach_synchronously needed only for local env, CI has it ON
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&database_atomic_wait_for_drop_and_detach_synchronously=1" -d "$*"
}


# The query is not atomic - it can compare states between system.parts and system.metrics from different points in time.
# So, there is inherent race condition. But it should get expected result eventually.
# In case of test failure, this code will do infinite loop and timeout.
verify()
{
    local result

    for _ in {1..100}; do
        # NOTE: database = $CLICKHOUSE_DATABASE is unwanted
        result=$( query "SELECT
            (SELECT sumIf(value, metric = 'PartsActive'), sumIf(value, metric = 'PartsOutdated') FROM system.metrics)
                =
            (SELECT sum(active), sum(NOT active) FROM (
                SELECT active FROM system.parts
                UNION ALL SELECT active FROM system.projection_parts
                UNION ALL SELECT active FROM system.dropped_tables_parts
            ))"
        )

        if [ "$result" = "1" ]; then
            echo "$result"
            return
        fi

        sleep 0.5
    done

    $CLICKHOUSE_CLIENT -q "
        SELECT sumIf(value, metric = 'PartsActive'), sumIf(value, metric = 'PartsOutdated') FROM system.metrics;
        SELECT sum(active), sum(NOT active) FROM system.parts;
        SELECT sum(active), sum(NOT active) FROM system.projection_parts;
        SELECT sum(active), sum(NOT active) FROM system.dropped_tables_parts;
    "
}

query "DROP TABLE IF EXISTS test_table"
query "CREATE TABLE test_table (data Date) ENGINE = MergeTree PARTITION BY toYear(data) ORDER BY data;"

query "INSERT INTO test_table VALUES ('1992-01-01')"
verify

query "INSERT INTO test_table VALUES ('1992-01-02')"
verify

query "OPTIMIZE TABLE test_table FINAL"
verify

query "DROP TABLE test_table"
verify
