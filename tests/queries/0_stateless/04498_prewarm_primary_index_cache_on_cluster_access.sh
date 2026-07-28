#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

index_user="index_user_04498_$CLICKHOUSE_DATABASE"
mark_user="mark_user_04498_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $index_user, $mark_user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_04498"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_04498 (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS use_primary_key_cache = 1"

${CLICKHOUSE_CLIENT} --query "CREATE USER $index_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM PREWARM PRIMARY INDEX CACHE ON *.* TO $index_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $mark_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM PREWARM MARK CACHE ON *.* TO $mark_user"

query="SYSTEM PREWARM PRIMARY INDEX CACHE ON CLUSTER test_shard_localhost $CLICKHOUSE_DATABASE.t_04498"

# The ON CLUSTER path must check SYSTEM PREWARM PRIMARY INDEX CACHE, not SYSTEM PREWARM MARK CACHE.
# Holder of the primary index cache grant is allowed; holder of only the mark cache grant is denied.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "$index_user" --query "$query"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "$mark_user" --query "$query -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "DROP USER $index_user, $mark_user"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_04498"
