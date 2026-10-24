#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the short ATTACH VIEW is rejected in a Replicated database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}/overridden" "${WORKING_FOLDER}/default"

QUERIES="SELECT getServerSetting('ignore_empty_sql_security_in_create_view_query');
CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW v AS SELECT k FROM t;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY k AS SELECT k FROM t;
INSERT INTO t VALUES (1);
DETACH VIEW v;
ATTACH VIEW v;
SELECT * FROM v;
DETACH VIEW mv;
ATTACH MATERIALIZED VIEW mv;
SELECT count() FROM mv;
DROP VIEW v;
DROP VIEW mv;
DROP TABLE t;
SELECT count() FROM system.tables WHERE database = currentDatabase();"

${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}/overridden" --query "${QUERIES}" -- --ignore_empty_sql_security_in_create_view_query=0
${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}/default" --query "${QUERIES}"

rm -rf "${WORKING_FOLDER}"
