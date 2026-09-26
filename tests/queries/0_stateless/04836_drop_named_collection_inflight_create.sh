#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `create_table_pause_before_commit` failpoint is process-global and would pause the
# first `CREATE TABLE` of any concurrently running test.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local, but the test cluster executes
# the paused `CREATE TABLE` on all replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The dependencies of a table are registered while its engine arguments are resolved, before the table
# is committed to the catalog. A concurrent `DROP NAMED COLLECTION` must not classify the dependency of
# such an in-flight `CREATE`/`ATTACH` as a leftover of a failed create: the create would then succeed
# with the collection gone, and the `ATTACH` replayed at the next server start would throw
# `NAMED_COLLECTION_DOESNT_EXIST` and the server would not start. The drop synchronizes on the
# `DDLGuard` of the table name, which the creating query holds for the whole window.

NC="nc_${CLICKHOUSE_DATABASE}"

drop_log="${CLICKHOUSE_TMP}/drop_${CLICKHOUSE_DATABASE}.log"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_table_pause_before_commit" 2>/dev/null ||:
}
trap cleanup EXIT

function report_drop_result()
{
    if grep -q "NAMED_COLLECTION_IS_USED" "$drop_log"
    then
        echo "NAMED_COLLECTION_IS_USED"
    else
        cat "$drop_log"
    fi
}

echo "--- DROP NAMED COLLECTION during an in-flight CREATE TABLE ---"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV'"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT create_table_pause_before_commit"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t (x UInt32) ENGINE = URL(${NC})" &
create_pid=$!

# The create has registered its dependency and paused just before committing the table.
${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_table_pause_before_commit PAUSE"

# The drop sees the dependency of a table that is not in the catalog and blocks on the `DDLGuard`
# the paused create holds; it must not treat the dependency as stale and drop the collection.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" > "$drop_log" 2>&1 &
drop_pid=$!

# Release the create once the drop either waits on the guard (visible in the process list) or has
# already terminated (only possible if it wrongly pruned the dependency and dropped the collection).
while kill -0 "$drop_pid" 2>/dev/null \
    && [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query LIKE 'DROP NAMED COLLECTION ${NC}%'") -eq 0 ]]
do
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_table_pause_before_commit"

wait "$create_pid"
wait "$drop_pid" ||:
report_drop_result

# The create committed the table, and the collection survived with the metadata still valid.
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't';
"

echo "--- DROP NAMED COLLECTION during an in-flight ATTACH TABLE ---"
${CLICKHOUSE_CLIENT} -m -q "
DETACH TABLE t;
SYSTEM ENABLE FAILPOINT create_table_pause_before_commit;
"

${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t" &
attach_pid=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_table_pause_before_commit PAUSE"

# The attach has registered its dependency again, and the record of the detached table also still
# exists; both must keep refusing the drop.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" > "$drop_log" 2>&1 &
drop_pid=$!

while kill -0 "$drop_pid" 2>/dev/null \
    && [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query LIKE 'DROP NAMED COLLECTION ${NC}%'") -eq 0 ]]
do
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_table_pause_before_commit"

wait "$attach_pid"
wait "$drop_pid" ||:
report_drop_result

${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
