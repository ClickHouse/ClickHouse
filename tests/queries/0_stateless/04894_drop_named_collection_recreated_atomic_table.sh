#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `create_table_pause_before_commit` failpoint is process-global and would pause the
# first `CREATE TABLE` of any concurrently running test.
# no-replicated-database: `SYSTEM ENABLE FAILPOINT` is process-local, but the test cluster executes
# the paused `CREATE TABLE` on all replicas; explicit UUIDs are also forbidden there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failed CREATE of an Atomic table leaves a dependency keyed by its UUID. A later CREATE of the same
# table name gets a new UUID, so the stale entry does not identify the committed table. The stale-entry
# cleanup must nevertheless see the new live dependency and refuse the drop.

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

old_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
new_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

echo "--- a failed CREATE leaves a stale dependency for the old UUID ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE TABLE t UUID '${old_uuid}' (x UInt32) ENGINE = URL(${NC}); -- { serverError UNKNOWN_FORMAT }
ALTER NAMED COLLECTION ${NC} SET format = 'CSV';
SYSTEM ENABLE FAILPOINT create_table_pause_before_commit;
"

# The new table registers its dependency before it pauses, then commits after the drop starts waiting on
# the DDLGuard. It has the same name but a different UUID from the failed create.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t UUID '${new_uuid}' (x UInt32) ENGINE = URL(${NC})" &
create_pid=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_table_pause_before_commit PAUSE"

${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}" > "$drop_log" 2>&1 &
drop_pid=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_table_pause_before_commit"
wait "$create_pid"
wait "$drop_pid" ||:
report_drop_result

${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT count() FROM system.named_collections WHERE name = '${NC}';
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
