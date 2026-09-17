#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on the local user_files directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for the unified `URL` engine scheme dispatch: the dispatch wrapper must persist the
# delegate's inferred data format into the stored `URL(...)` arguments, mirroring the plain `URL`
# engine. Otherwise an extensionless `format = auto` URL is stored without a format, and on
# `ATTACH`/restart the delegate is rebuilt with `format = auto` and re-reads the external resource
# to rediscover the format even though the schema is already persisted.

NOEXT="${CLICKHOUSE_TEST_UNIQUE_NAME}_noext"
ABS_NOEXT="${USER_FILES_PATH}/${NOEXT}"
# Distinctive JSON content so the format is inferred deterministically from the data, not the name.
printf '{"a":1,"b":"Hello"}\n{"a":2,"b":"World"}\n' > "$ABS_NOEXT"

echo "--- ENGINE = URL('file://<extensionless>') with format = auto persists the inferred format ---"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_f ENGINE = URL('file://${ABS_NOEXT}')"
# The persisted engine definition carries a concrete format, not 'auto'.
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query NOT ILIKE '%auto%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_f'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"

echo "--- reload after the source file is removed succeeds (no format re-inference) ---"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"
rm -f "$ABS_NOEXT"
# With the format persisted, ATTACH does not re-read the file to rediscover the format (the schema
# is already stored), so the reload succeeds even though the source file is gone.
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_f'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_f"

echo "--- an explicitly given format is preserved (not overwritten) ---"
ABS_CSV="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_explicit"
printf '1,Hello\n2,World\n' > "$ABS_CSV"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_e"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_e (a UInt32, b String) ENGINE = URL('file://${ABS_CSV}', 'CSV')"
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query LIKE '%CSV%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_e'"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_e ORDER BY a"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_e"
rm -f "$ABS_CSV"

echo "--- ENGINE = URL(named_collection) with format = auto persists the inferred format ---"
NC="${CLICKHOUSE_TEST_UNIQUE_NAME}_nc"
NOEXT_NC="${CLICKHOUSE_TEST_UNIQUE_NAME}_noext_nc"
ABS_NOEXT_NC="${USER_FILES_PATH}/${NOEXT_NC}"
printf '{"a":1,"b":"Hello"}\n{"a":2,"b":"World"}\n' > "$ABS_NOEXT_NC"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS url = 'file://${ABS_NOEXT_NC}'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n ENGINE = URL(${NC})"
# The persisted engine definition carries a concrete format, not 'auto'.
${CLICKHOUSE_CLIENT} -q "SELECT create_table_query NOT ILIKE '%auto%' FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_n'"
# Reload after the source file is removed succeeds (no format re-inference).
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
rm -f "$ABS_NOEXT_NC"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_n'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_n"
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${NC}"
