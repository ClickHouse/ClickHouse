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

# The cleanup of a stale dependency (a leftover of a failed `CREATE TABLE`) synchronizes on the
# `DDLGuard` of the table name the dependency recorded, but `CREATE TABLE ... UUID` can reuse the UUID
# of the failed create under a different table name, and the guard of the old name proves nothing about
# such a create. The cleanup must remove only the exact stale entry: removing everything under the UUID
# would erase the live dependency of the in-flight create, and the collection it uses could then be
# dropped from under the committed table.

OLD_NC="old_nc_${CLICKHOUSE_DATABASE}"
NEW_NC="new_nc_${CLICKHOUSE_DATABASE}"

drop_log="${CLICKHOUSE_TMP}/drop_${CLICKHOUSE_DATABASE}.log"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_table_pause_before_commit" 2>/dev/null ||:
}
trap cleanup EXIT

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

echo "--- a failed CREATE TABLE ... UUID leaves a stale dependency ---"
# The collection resolves during the engine argument resolution (the dependency is registered), and the
# storage constructor then rejects the unknown format: the create fails, leaving a stale dependency
# that carries both the table name and the explicit UUID.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE TABLE old_t UUID '${uuid}' (x UInt32) ENGINE = URL(${OLD_NC}); -- { serverError UNKNOWN_FORMAT }
"

echo "--- DROP of the old collection during an in-flight CREATE reusing the UUID ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NEW_NC} AS url = 'http://localhost:8123', format = 'CSV';
SYSTEM ENABLE FAILPOINT create_table_pause_before_commit;
"

# The create registers its dependency on the new collection under (new_t, uuid) and pauses just before
# committing the table.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE new_t UUID '${uuid}' (x UInt32) ENGINE = URL(${NEW_NC})" &
create_pid=$!

${CLICKHOUSE_CLIENT} -q "SYSTEM WAIT FAILPOINT create_table_pause_before_commit PAUSE"

# The drop sees only the stale entry of `old_t`; nothing holds the `DDLGuard` of that name, so the drop
# prunes the entry and succeeds - but it must remove only that exact entry, not everything recorded
# under the UUID, which the in-flight create of `new_t` is reusing.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${OLD_NC}" > "$drop_log" 2>&1
cat "$drop_log"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT create_table_pause_before_commit"
wait "$create_pid"

echo "--- the committed table still protects its collection ---"
${CLICKHOUSE_CLIENT} -m -q "
SELECT count() FROM system.named_collections WHERE name = '${OLD_NC}';
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'new_t';
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
DROP TABLE new_t;
DROP NAMED COLLECTION ${NEW_NC};
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
"
