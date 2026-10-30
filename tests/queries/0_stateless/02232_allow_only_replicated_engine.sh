#!/usr/bin/env bash
# Tags: replica, no-shared-merge-tree
# no-shared-merge-tree: `ReplicatedMergeTree` is replaced with `SharedMergeTree`, which the test user has no `TABLE ENGINE` grant for

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
# `CREATE TABLE ... AS file(...)` with an inferred structure creates the table through a lazy table function
# proxy, which must be rejected without executing the table function, like a `File` table over an explicit path.
${CLICKHOUSE_CLIENT} --query "INSERT INTO TABLE FUNCTION file('${CLICKHOUSE_DATABASE}/tab_file_as.csv', 'CSV', 'x UInt32') SELECT 1"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_file_as AS file('${CLICKHOUSE_DATABASE}/tab_file_as.csv', 'CSV');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_file_as_static AS file('${CLICKHOUSE_DATABASE}/tab_file_as.csv', 'CSV', 'x UInt32');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
# The check runs before the structure is inferred from the source, so a missing file is rejected by the
# policy rather than by a file or schema inference error.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_file_as_missing AS file('${CLICKHOUSE_DATABASE}/tab_file_as_missing.csv', 'CSV');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
# The same holds for `url('file://...')`, which delegates to the `file` table function.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_url_file_as AS url('file://${CLICKHOUSE_DATABASE}/tab_file_as.csv', 'CSV');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_url_file_as_static AS url('file://${CLICKHOUSE_DATABASE}/tab_file_as.csv', 'CSV', 'x UInt32');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
# A table engine definition without a column list infers the schema while the storage is constructed, so `File` and
# `URL('file://...')` are rejected before the construction: a missing file is rejected by the policy, not by a schema error.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_file_engine_missing engine = File(CSV, '${CLICKHOUSE_DATABASE}/tab_file_as_missing.csv');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_url_file_engine_missing engine = URL('file://${CLICKHOUSE_DATABASE}/tab_file_as_missing.csv', 'CSV');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
# An absolute `http` URL does not store data on disk and is allowed. A relative URL is rejected conservatively:
# `url_base` is not persisted, so another replica could resolve it to a different backend, e.g. to `file://`.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_url_http_as AS url('http://127.0.0.1:1/data.csv', 'CSV', 'x UInt32');"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_only_replicated_engine=1 --url_base='http://127.0.0.1:1/' --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_db.tab_url_relative_as AS url('data.csv', 'CSV', 'x UInt32');" 2>&1 | grep -o "Only tables with a replicated engine or tables which do not store their own data on disk" | head -n 1
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_db"
${CLICKHOUSE_CLIENT} -q "DROP USER user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "drop table mute_stylecheck"
