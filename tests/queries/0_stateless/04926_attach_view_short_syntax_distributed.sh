#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# zookeeper, no-fasttest: needs ZooKeeper for the Replicated database engine.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db="rdb_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "DROP DATABASE IF EXISTS ${db} SYNC"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE DATABASE ${db} ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/rdb_04926', 's1', 'r1')"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --multiquery --query "
CREATE TABLE ${db}.src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ${db}.dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW ${db}.v AS SELECT k FROM ${db}.src;
CREATE MATERIALIZED VIEW ${db}.mv TO ${db}.dst AS SELECT k FROM ${db}.src;
DETACH VIEW ${db}.v PERMANENTLY;
DETACH VIEW ${db}.mv PERMANENTLY;
"

for version in 1 5; do
    ${CLICKHOUSE_CLIENT} --distributed_ddl_entry_format_version "$version" --query "ATTACH VIEW ${db}.v" 2>&1 \
        | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
    ${CLICKHOUSE_CLIENT} --distributed_ddl_entry_format_version "$version" --query "ATTACH MATERIALIZED VIEW ${db}.mv" 2>&1 \
        | grep -q "INCORRECT_QUERY" && echo "INCORRECT_QUERY" || echo "NO ERROR"
done

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = '${db}' AND name IN ('v', 'mv')"

# A temporary object belongs to no database, so it must keep the rejection that names it rather than
# the one about Replicated databases. `USE` switches the current database, because `CLICKHOUSE_CLIENT`
# already carries `--database` and it cannot be given twice.
${CLICKHOUSE_CLIENT} --multiquery --query "
USE ${db};
ATTACH TEMPORARY VIEW tv;
" 2>&1 | grep -q "SYNTAX_ERROR" && echo "SYNTAX_ERROR" || echo "NO ERROR"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --multiquery --query "
ATTACH TABLE ${db}.v;
ATTACH TABLE ${db}.mv;
"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = '${db}' AND name IN ('v', 'mv') ORDER BY name"
${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'TO [^ ]+') = 'TO ${db}.dst' FROM system.tables WHERE database = '${db}' AND name = 'mv'"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "DROP DATABASE ${db} SYNC"
