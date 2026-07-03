#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RENAME DATABASE must update the folded database-name index used by
# case_insensitive_names = 'standard': the new name resolves case-insensitively,
# the old folded name does not.
# Database names embed ${CLICKHOUSE_DATABASE} so parallel test instances don't collide;
# the mixed-case suffix is what exercises the folded lookup.

DB_FOO="${CLICKHOUSE_DATABASE}_RenCaseFoo"
DB_BAR="${CLICKHOUSE_DATABASE}_RenCaseBar"
DB_FOO_FOLDED=$(echo "$DB_FOO" | tr '[:upper:]' '[:lower:]')
DB_BAR_FOLDED=$(echo "$DB_BAR" | tr '[:upper:]' '[:lower:]')

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_FOO}"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB_BAR}"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB_FOO}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB_FOO}.t (x Int32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${DB_FOO}.t VALUES (7)"
${CLICKHOUSE_CLIENT} --query "RENAME DATABASE ${DB_FOO} TO ${DB_BAR}"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_BAR_FOLDED}.t SETTINGS case_insensitive_names = 'standard'"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM ${DB_FOO_FOLDED}.t SETTINGS case_insensitive_names = 'standard'" 2>&1 | grep -oF "UNKNOWN_DATABASE"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB_BAR}"
