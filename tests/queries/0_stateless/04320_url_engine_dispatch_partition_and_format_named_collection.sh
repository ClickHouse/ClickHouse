#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: relies on the local user_files directory.
# no-replicated-database: named collections are server-global, not database-scoped

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "--- ENGINE = URL(named_collection) with format = auto persists the inferred format ---"
NC="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc"
NOEXT_NC="${CLICKHOUSE_TEST_UNIQUE_NAME}_noext_nc"
ABS_NOEXT_NC="${USER_FILES_PATH}/${NOEXT_NC}"
printf '{"a":1,"b":"Hello"}\n{"a":2,"b":"World"}\n' > "$ABS_NOEXT_NC"
# Reuse a leftover collection rather than recreating it: dropping it is refused while a leftover table
# still references it.
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION IF NOT EXISTS ${NC} AS url = 'file://${ABS_NOEXT_NC}'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n ENGINE = URL(${NC})"
# The persisted engine definition carries a concrete format, not 'auto' (quoted literal, so that the
# random database name in the DDL cannot match).
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query NOT ILIKE '%''auto''%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_n'"
# Reload after the source file is removed succeeds (no format re-inference).
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
rm -f "$ABS_NOEXT_NC"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_n'"
# Chained so the collection outlives the table: metadata must never reference a missing collection.
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n" && ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}"
