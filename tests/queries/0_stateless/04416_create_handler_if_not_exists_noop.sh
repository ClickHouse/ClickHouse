#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# CREATE HANDLER IF NOT EXISTS on an existing handler must be a true no-op: it must not fail while
# validating the unused replacement query, e.g. an invalid regexp URL or an unsupported TYPE. The
# existence check therefore runs before the replacement is built.

# Per-test-unique names/URLs so parallel runs do not interfere (handlers are a global namespace).
H="h_04416_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS ${H}"
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER ${H} URL '/${H}' AS SELECT 1 AS original" && echo "created"

# Invalid regexp in the replacement query: a no-op because the handler already exists
# (previously failed with CANNOT_COMPILE_REGEXP while compiling the unused regexp).
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER IF NOT EXISTS ${H} URL REGEXP '(' AS SELECT 2 AS replaced" && echo "noop_regexp"

# Unsupported TYPE in the replacement: also a no-op (previously failed with BAD_ARGUMENTS).
${CLICKHOUSE_CLIENT} -q "CREATE HANDLER IF NOT EXISTS ${H} URL '/${H}_other' TYPE static AS SELECT 2 AS replaced" && echo "noop_type"

# The existing handler is untouched: still the original query, exactly one row.
${CLICKHOUSE_CLIENT} -q "SELECT count(), any(query) FROM system.handlers WHERE name = '${H}'"

${CLICKHOUSE_CLIENT} -q "DROP HANDLER ${H}" && echo "dropped"
