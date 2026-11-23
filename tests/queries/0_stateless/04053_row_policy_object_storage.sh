#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest

# Regression test: row policy on a URL table with Parquet format caused
# "Logical error: 'prewhere_info'" because updateFormatPrewhereInfo asserted
# prewhere_info was non-null, but only row_level_filter was set.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04053_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE IF EXISTS ${db}.source_data;
CREATE TABLE ${db}.source_data (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.source_data VALUES (1, 'a'), (2, 'b'), (3, 'c');

DROP TABLE IF EXISTS ${db}.url_parquet;
CREATE TABLE ${db}.url_parquet (id UInt32, value String)
ENGINE = URL('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+*+FROM+${db}.source_data+FORMAT+Parquet', 'Parquet');

DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT SELECT ON ${db}.url_parquet TO ${user};

DROP ROW POLICY IF EXISTS rp_04053 ON ${db}.url_parquet;
CREATE ROW POLICY rp_04053 ON ${db}.url_parquet FOR SELECT USING id <= 2 TO ${user};
EOF

echo "--- Row policy filters URL Parquet table ---"
${CLICKHOUSE_CLIENT} --user ${user} --query "SELECT * FROM ${db}.url_parquet ORDER BY id"

echo "--- Row policy with WHERE on URL Parquet table ---"
${CLICKHOUSE_CLIENT} --user ${user} --query "SELECT * FROM ${db}.url_parquet WHERE value = 'a' ORDER BY id"

echo "--- Row policy count on URL Parquet table ---"
${CLICKHOUSE_CLIENT} --user ${user} --query "SELECT count() FROM ${db}.url_parquet"

${CLICKHOUSE_CLIENT} <<EOF
DROP ROW POLICY IF EXISTS rp_04053 ON ${db}.url_parquet;
DROP USER IF EXISTS ${user};
DROP TABLE IF EXISTS ${db}.url_parquet;
DROP TABLE IF EXISTS ${db}.source_data;
EOF
