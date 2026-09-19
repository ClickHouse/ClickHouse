#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

embedded_user="embedded_user_05214_$CLICKHOUSE_DATABASE"
dictionary_user="dictionary_user_05214_$CLICKHOUSE_DATABASE"
no_privilege_user="no_privilege_user_05214_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $embedded_user, $dictionary_user, $no_privilege_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $embedded_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM RELOAD EMBEDDED DICTIONARIES ON *.* TO $embedded_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $dictionary_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER, SYSTEM RELOAD DICTIONARY ON *.* TO $dictionary_user"

${CLICKHOUSE_CLIENT} --query "CREATE USER $no_privilege_user IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} --query "GRANT CLUSTER ON *.* TO $no_privilege_user"

${CLICKHOUSE_CLIENT} --user "$embedded_user" --query "SYSTEM RELOAD EMBEDDED DICTIONARIES"

query="SYSTEM RELOAD EMBEDDED DICTIONARIES ON CLUSTER test_shard_localhost"

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "$embedded_user" --query "$query"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "$dictionary_user" --query "$query"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none --user "$no_privilege_user" --query "$query -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} --query "DROP USER $embedded_user, $dictionary_user, $no_privilege_user"
