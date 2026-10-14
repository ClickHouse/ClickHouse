#!/usr/bin/env bash
# Tags: no-parallel, no-flaky-check, no-replicated-database
# Tag no-parallel: the test creates the fixed global databases `db_01939` and
# `NONE` and the fixed global user `u_01939`, and it drops the `NONE` database
# at the end, so a concurrent copy of the test would race on those names.


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT_BINARY}  --query "create database if not exists db_01939"
${CLICKHOUSE_CLIENT_BINARY}  --query "create database if not exists NONE"

#create user by sql
${CLICKHOUSE_CLIENT_BINARY}  --query "drop user if exists u_01939"
${CLICKHOUSE_CLIENT_BINARY}  --query "create user u_01939 default database db_01939"

${CLICKHOUSE_CLIENT_BINARY}  --query "SELECT currentDatabase();"
${CLICKHOUSE_CLIENT_BINARY}  --user=u_01939 --query "SELECT currentDatabase();"

${CLICKHOUSE_CLIENT_BINARY}  --query "alter user u_01939 default database NONE"
${CLICKHOUSE_CLIENT_BINARY}  --query "show create user u_01939"
${CLICKHOUSE_CLIENT_BINARY}  --query "alter user u_01939 default database \`NONE\`"
${CLICKHOUSE_CLIENT_BINARY}  --query "show create user u_01939"

${CLICKHOUSE_CLIENT_BINARY}  --query "drop user u_01939 "
${CLICKHOUSE_CLIENT_BINARY}  --query "drop database db_01939"
${CLICKHOUSE_CLIENT_BINARY}  --query "drop database NONE"
