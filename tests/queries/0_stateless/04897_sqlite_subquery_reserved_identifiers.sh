#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$(mktemp "$CLICKHOUSE_TMP/sqlite_subquery_reserved_identifiers_XXXXXX.sqlite")
trap 'rm -f "$DB"' EXIT

sqlite3 "$DB" 'CREATE TABLE "group" ("where" INTEGER); INSERT INTO "group" VALUES (42);'

${CLICKHOUSE_LOCAL} --query="SELECT * FROM sqlite('${DB}', (SELECT \`where\` FROM \`group\`))"
