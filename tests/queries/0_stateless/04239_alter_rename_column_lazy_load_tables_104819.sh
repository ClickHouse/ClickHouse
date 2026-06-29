#!/usr/bin/env bash

# Regression test for #104819 (STID 0993-2385).
#
# An `ALTER RENAME COLUMN` on a `lazy_load_tables=1` `DatabaseAtomic` after
# `DETACH DATABASE` / `ATTACH DATABASE` left the `StorageTableProxy`'s
# cached metadata out of sync with the nested storage. When the alter was
# blocked on a `STOP MERGES` mutation, `setProperties` on the nested
# storage had already published the post-rename schema (`c1`) while the
# proxy still returned the pre-rename schema (`c0`) from
# `getInMemoryMetadataPtr`. A concurrent `INSERT` then resolved column
# names against the proxy's stale view but built its sink from the nested
# storage's current metadata, and the chain raised
# `Block structure mismatch ... different names of columns: c0 ... c1` in
# `Chain::addSink`.
#
# A direct `INSERT`-driven repro kills the server in debug/sanitizer
# builds, which `clickhouse-test` records as `server-died` (mapped to
# `ERROR`, not `FAIL`). The bugfix-validation framework only inverts
# `FAIL <-> OK`, so a server-death result reads as "Failed to reproduce
# the bug". We therefore probe the underlying metadata mismatch with
# `DESCRIBE TABLE` (goes through `IStorage::getInMemoryMetadataPtr` on
# the proxy) and compare it to the on-disk `CREATE TABLE` (rewritten by
# `setProperties` as part of the alter). Without the fix the two
# disagree; with the fix the proxy forwards to the nested storage and
# they match.

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

# Drive `ALTER RENAME COLUMN` from a background client. With merges
# stopped the server-side alter blocks on the rename mutation right
# after `setProperties` has already updated the nested storage's
# in-memory metadata and rewritten the on-disk `CREATE TABLE`. Use
# short client timeouts so the client disconnects fast; the server-side
# alter stays parked until we resume merges below.
${CLICKHOUSE_CLIENT} --receive_timeout 2 --send_timeout 2 \
    -q "ALTER TABLE \`${DB}\`.t RENAME COLUMN c0 TO c1" >/dev/null 2>&1 &
ALTER_PID=$!

# Wait for the race window to open. `SHOW CREATE TABLE` reads the table's
# AST via the database engine, which is updated synchronously by
# `setProperties` before the alter blocks on the mutation. Once it
# returns `c1` we know `nested.getInMemoryMetadataPtr` already exposes
# `c1`; the proxy's cached metadata is still `c0` unless the fix is in
# place.
for _ in $(seq 1 200); do
    create=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE TABLE \`${DB}\`.t" 2>/dev/null)
    case "${create}" in
        *'`c1`'*) break ;;
    esac
    sleep 0.05
done

# Probe the proxy via `DESCRIBE TABLE`. `InterpreterDescribeQuery` calls
# `storage->getInMemoryMetadataPtr`, which hits `StorageTableProxy`.
# - Without the fix: returns the proxy's stale cached metadata -> `c0`.
# - With the fix:    forwards to the nested storage             -> `c1`.
# The reference fixes the expected column name to `c1`, so the buggy
# build produces a clean output diff (regular `FAIL`) instead of crashing
# the server.
${CLICKHOUSE_CLIENT} -q "DESCRIBE TABLE \`${DB}\`.t FORMAT TSV" \
    | awk '{print $1}'

# Resume merges so the parked alter mutation can complete, then wait for
# the background client process to wind down before dropping.
${CLICKHOUSE_CLIENT} -q "SYSTEM START MERGES \`${DB}\`.t" >/dev/null
wait "${ALTER_PID}" 2>/dev/null || true

${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${DB}\` SYNC"
