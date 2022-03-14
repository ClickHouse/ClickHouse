#!/usr/bin/env bash
# Tags: replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "CREATE USER user_${CLICKHOUSE_DATABASE} settings database_replicated_allow_only_replicated_engine=1"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}_db.* TO user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --allow_experimental_database_replicated=1 --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db engine = Replicated('/clickhouse/databases/${CLICKHOUSE_DATABASE}_db', '{shard}', '{replica}')"
${CLICKHOUSE_CLIENT} --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_memory (x UInt32) engine = Memory;"
${CLICKHOUSE_CLIENT} --user "user_${CLICKHOUSE_DATABASE}" -n --query "set distributed_ddl_entry_format_version=2; CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_mt (x UInt32) engine = MergeTree order by x;" 2>&1 | grep -o "Only table with Replicated engine"
${CLICKHOUSE_CLIENT} -n --query "set distributed_ddl_entry_format_version=2; CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_mt (x UInt32) engine = MergeTree order by x;"
${CLICKHOUSE_CLIENT} --user "user_${CLICKHOUSE_DATABASE}" -n --query "set distributed_ddl_entry_format_version=2; CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_rmt (x UInt32) engine = ReplicatedMergeTree order by x;"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} -q "DROP USER user_${CLICKHOUSE_DATABASE}"
