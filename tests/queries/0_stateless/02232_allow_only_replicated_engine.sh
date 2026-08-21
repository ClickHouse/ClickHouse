#!/usr/bin/env bash
# Tags: replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "create table mute_stylecheck (x UInt32) engine = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/root', '1') order by x"

${CLICKHOUSE_CLIENT} -q "CREATE USER user_${CLICKHOUSE_DATABASE} settings database_replicated_allow_only_replicated_engine=1"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}_db.* TO user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON Memory, TABLE ENGINE ON Null, TABLE ENGINE ON Set, TABLE ENGINE ON Join, TABLE ENGINE ON File, TABLE ENGINE ON MergeTree, TABLE ENGINE ON ReplicatedMergeTree, TABLE ENGINE ON Distributed TO user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "GRANT FILE ON *.* TO user_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db engine = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_db', '{shard}', '{replica}')"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_memory (x UInt32) engine = Memory;"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_null (x UInt32) engine = Null;"
# Engines that keep their own unreplicated data on disk are rejected.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_mt (x UInt32) engine = MergeTree order by x;" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_set (x UInt32) engine = Set;" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
# `File` over an explicit path keeps the table data in a local unreplicated file, exactly like a `File` table
# inside the database directory, so it is rejected as well.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_file (x UInt32) engine = File(CSV, '${CLICKHOUSE_DATABASE}/tab_file.csv');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_mt (x UInt32) engine = MergeTree order by x;"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_rmt (x UInt32) engine = ReplicatedMergeTree order by x;"
# With `persistent = 0` the `Set` and `Join` engines keep their data only in memory, like `Memory`, so they are allowed.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_set_in_memory (x UInt32) engine = Set SETTINGS persistent = 0;"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_join_in_memory (x UInt32, y UInt32) engine = Join(ANY, LEFT, x) SETTINGS persistent = 0;"
# `Distributed`, `Remote`, and `RemoteSecure` keep table data remotely. Their local background `INSERT`
# queue is a transient send buffer, not data of the table itself, so these engines are allowed.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --user "user_${CLICKHOUSE_DATABASE}" --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_dist (x UInt32) engine = Distributed(test_shard_localhost, '${CLICKHOUSE_DATABASE}_db', tab_rmt, x);"

# The CREATE TABLE ... AS ... variant infers the structure from another table and takes
# a different code path in the interpreter. Run it with the setting passed at the query
# level (the default user has enough privileges to read the source table's structure).
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_dist_as AS ${CLICKHOUSE_DATABASE}_db.tab_rmt engine = Distributed(test_shard_localhost, '${CLICKHOUSE_DATABASE}_db', tab_rmt, x);"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_remote (x UInt32) engine = Remote('127.0.0.1', '${CLICKHOUSE_DATABASE}_db', tab_rmt);"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_remote_numbers (number UInt64) engine = Remote('127.0.0.1', numbers(10));"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} -q "DROP USER user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "drop table mute_stylecheck"
