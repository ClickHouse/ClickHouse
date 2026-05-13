#!/usr/bin/env bash

# Regression for #104819 (STID 0993-2385).
# An `ALTER RENAME COLUMN` on a `lazy_load_tables=1` `DatabaseAtomic` after
# `DETACH DATABASE` / `ATTACH DATABASE` left the `StorageTableProxy`'s cached
# metadata out of sync with the nested storage while the alter was running.
# A concurrent `INSERT` with the pre-rename column name would resolve against
# the stale proxy metadata, build a pipeline source with the old column, then
# build a sink from the nested storage with the new column, and crash the
# server with `Logical error: 'Block structure mismatch ... different names of
# columns: c0 ... c1'` in `Chain::addSink`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy_rename"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${DB}\` SYNC"
${CLICKHOUSE_CLIENT} -nq "
    CREATE DATABASE \`${DB}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${DB}\`.t (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
    INSERT INTO \`${DB}\`.t (c0) VALUES (1);
    DETACH DATABASE \`${DB}\` SYNC;
    ATTACH DATABASE \`${DB}\`;
    SYSTEM STOP MERGES \`${DB}\`.t;
"

# Start `ALTER RENAME COLUMN` in the background. With merges stopped, the
# alter blocks waiting for the column-rename mutation to finish, leaving the
# proxy's cached metadata behind. Use a short client timeout so the client
# disconnects quickly; the alter call continues on the server side until we
# restart merges below.
${CLICKHOUSE_CLIENT} --receive_timeout 2 --send_timeout 2 \
    -q "ALTER TABLE \`${DB}\`.t RENAME COLUMN c0 TO c1" >/dev/null 2>&1 &
ALTER_PID=$!

# Give the server time to enter the alter call and run `setProperties` on the
# nested storage. After this point the nested has the new schema (`c1`) but
# the proxy's cached metadata still has `c0` -- this is the race window. We
# poll `system.columns` to detect that the nested has been updated (and the
# proxy used to show the stale view), with a hard upper bound to keep the
# test bounded if the race never opens (e.g. if the alter completes too
# quickly).
for _ in $(seq 1 50); do
    cnt=$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.columns WHERE database = '${DB}' AND table = 't' AND name = 'c1'" 2>/dev/null)
    [[ "${cnt}" == "1" ]] && break
    sleep 0.1
done

# Before the fix this `INSERT` crashed the server with the
# `Block structure mismatch` `LOGICAL_ERROR`. After the fix the proxy reports
# the nested's current schema, so the `INSERT` resolves against the renamed
# column and fails cleanly with `NO_SUCH_COLUMN_IN_TABLE` (or succeeds, in
# the rare case where `setProperties` has not yet run -- which is also fine).
# Either way the server must stay alive.
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`${DB}\`.t (c0) VALUES (2)" 2>&1 \
    | grep -qE "(NO_SUCH_COLUMN_IN_TABLE|^$)" && echo "no_crash" || echo "unexpected"

# Server must still be alive after the `INSERT`.
${CLICKHOUSE_CLIENT} -q "SELECT 'alive'"

# Resume merges so the in-flight alter mutation can finally complete, then
# wait for the background client process to finish.
${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES \`${DB}\`.t" >/dev/null
wait "${ALTER_PID}" 2>/dev/null || true

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${DB}\` SYNC"
