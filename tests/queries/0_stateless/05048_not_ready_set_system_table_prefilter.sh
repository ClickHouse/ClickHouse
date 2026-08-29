#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `IN` subquery set is filled by `CreatingSetsStep`, which runs only once the pipeline runs, so a
# one-block system table cannot apply a predicate holding one while its pipeline is still being built.

# The set is unbuilt when the pipeline is built, so these go through the deferred path and must still
# select exactly the matching rows.
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.databases WHERE name IN (SELECT 'system')"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.databases WHERE name IN (SELECT '05048_no_such_database')"

# A subquery that a time limit stops in `break` mode leaves the set unbuilt and unbuildable.
BIG="(SELECT toString(number) FROM numbers(300000000))"

check() {
    echo -n "$1 $3: "
    $CLICKHOUSE_CLIENT --max_execution_time 0.3 --timeout_overflow_mode break \
        --query "SELECT count() FROM system.$1 WHERE $2 $3 $BIG FORMAT Null" 2>&1 \
        | grep -c -F 'Not-ready Set' || true
}

check databases       name     "IN"
check mutations       database "IN"
check iceberg_history database "IN"
check databases       name     "GLOBAL IN"

$CLICKHOUSE_CLIENT --query "SELECT 'alive'"
