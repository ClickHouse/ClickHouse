#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `CREATE HANDLER ... AS RESTORE ...` must be accepted with the default (GET-only) methods:
# `BackupsWorker` allows RESTORE under the `readonly = 2` mode that the HTTP execution path sets
# for safe methods, and rejects it only under the strict, user-set `readonly = 1`. So the
# create-time "mutating query needs a mutating method" gate must not fire for RESTORE.

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hr_${DB}"
P="/hr_${DB}"

${CLICKHOUSE_CLIENT} --query "DROP HANDLER IF EXISTS ${H}_restore"

# RESTORE over the default GET method: accepted at definition time.
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER ${H}_restore URL '${P}/restore' AS RESTORE TABLE ${DB}.t FROM Disk('backups', '${DB}.zip')"
${CLICKHOUSE_CLIENT} --query "SELECT methods FROM system.handlers WHERE name = '${H}_restore'"

# BACKUP follows the same contract and was already accepted; pin it alongside RESTORE.
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER ${H}_backup URL '${P}/backup' AS BACKUP TABLE ${DB}.t TO Disk('backups', '${DB}.zip')"
${CLICKHOUSE_CLIENT} --query "SELECT methods FROM system.handlers WHERE name = '${H}_backup'"

# A genuinely mutating query over GET-only methods must still be rejected at definition time.
${CLICKHOUSE_CLIENT} --query "CREATE HANDLER ${H}_insert URL '${P}/insert' AS INSERT INTO ${DB}.t VALUES (1)" 2>&1 | grep -o "BAD_ARGUMENTS" | head -1

${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${H}_restore"
${CLICKHOUSE_CLIENT} --query "DROP HANDLER ${H}_backup"
