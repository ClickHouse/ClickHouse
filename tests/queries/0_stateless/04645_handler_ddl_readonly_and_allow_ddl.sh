#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Handler DDL (`CREATE` / `ALTER` / `DROP HANDLER`) persists HTTP routing, so it must be rejected
# in read-only contexts and when DDL is prohibited: the handler access flags are part of
# `ContextAccess` `not_readonly_flags` and `ddl_flags`.

# Per-test-unique names and URL prefix so parallel tests do not interfere (handlers are global).
DB="${CLICKHOUSE_DATABASE}"
H="hro_${DB}"
P="/hro_${DB}"

# `readonly = 2` (the mode safe HTTP methods set) rejects all three handler DDLs.
${CLICKHOUSE_CLIENT} --readonly 2 --query "CREATE HANDLER ${H} URL '${P}' AS SELECT 1" 2>&1 | grep -o "READONLY" | head -1
${CLICKHOUSE_CLIENT} --readonly 2 --query "ALTER HANDLER ${H} AS SELECT 2" 2>&1 | grep -o "READONLY" | head -1
${CLICKHOUSE_CLIENT} --readonly 2 --query "DROP HANDLER ${H}" 2>&1 | grep -o "READONLY" | head -1

# The strict, user-set `readonly = 1` rejects them as well.
${CLICKHOUSE_CLIENT} --readonly 1 --query "CREATE HANDLER ${H} URL '${P}' AS SELECT 1" 2>&1 | grep -o "READONLY" | head -1

# The safe-method HTTP path: a `GET` request implies `readonly = 2`.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=CREATE%20HANDLER%20${H}%20URL%20%27${P}%27%20AS%20SELECT%201" | grep -o "READONLY" | head -1

# `allow_ddl = 0` prohibits handler DDL.
${CLICKHOUSE_CLIENT} --allow_ddl 0 --query "CREATE HANDLER ${H} URL '${P}' AS SELECT 1" 2>&1 | grep -o "QUERY_IS_PROHIBITED" | head -1
${CLICKHOUSE_CLIENT} --allow_ddl 0 --query "DROP HANDLER ${H}" 2>&1 | grep -o "QUERY_IS_PROHIBITED" | head -1

# No handler must have been created by any of the rejected queries.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.handlers WHERE name = '${H}'"
