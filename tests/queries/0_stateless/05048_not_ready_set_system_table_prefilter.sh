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

# `ReadFromSystemOneBlock` emits exactly the rows that `fillData` produces as a single chunk, so the
# `read_rows` recorded in `system.query_log` is a reliable witness of how many rows the predicate left:
# one row per matching database, plus the single row the subquery itself reads. An unpruned read would
# instead scale with the number of databases on the server. (`max_rows_to_read` is intentionally not
# used as the witness: it is not enforced for this single-chunk source.)
read_rows() {
    $CLICKHOUSE_CLIENT --query_id "$1" --ast_fuzzer_runs 0 --query "$2" > /dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT read_rows FROM system.query_log
        WHERE query_id = '$1' AND type = 'QueryFinish' AND current_database = currentDatabase()
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

echo -n "rows read for one matching database: "
read_rows "05048_prune_one_${CLICKHOUSE_DATABASE}" \
    "SELECT name FROM system.databases WHERE name IN (SELECT '${CLICKHOUSE_DATABASE}') FORMAT Null"
echo -n "rows read for no matching database: "
read_rows "05048_prune_none_${CLICKHOUSE_DATABASE}" \
    "SELECT name FROM system.databases WHERE name IN (SELECT '${CLICKHOUSE_DATABASE}_absent') FORMAT Null"

# A subquery that a time limit stops in `break` mode leaves the set unbuilt and unbuildable.
BIG="(SELECT toString(number) FROM numbers(300000000))"

# Report the client's exit status next to the count, so that a query failing for an unrelated reason
# is distinguishable from one that succeeded.
# The subquery has to keep reading until the time limit stops it, so the read limit must be lifted:
# the functional-test profile caps reads at 20M rows, and a read limit, unlike a time limit, still
# lets the set finish building.
check() {
    local out="${CLICKHOUSE_TMP}/05048_${CLICKHOUSE_DATABASE}_$1_${3// /_}.out"
    $CLICKHOUSE_CLIENT --max_rows_to_read 0 --max_execution_time 0.3 --timeout_overflow_mode break \
        --query "SELECT count() FROM system.$1 WHERE $2 $3 $BIG FORMAT Null" > "$out" 2>&1
    local rc=$?
    echo "$1 $3: $(grep -c -F 'Not-ready Set' "$out") $([ "$rc" -eq 0 ] && echo ok || echo "rc=$rc")"
    rm -f "$out"
}

check databases       name     "IN"
check mutations       database "IN"
check iceberg_history database "IN"
check databases       name     "GLOBAL IN"

$CLICKHOUSE_CLIENT --query "SELECT 'alive'"
