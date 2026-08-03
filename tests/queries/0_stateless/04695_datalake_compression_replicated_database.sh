#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-replicated-database
# `no-replicated-database`: the test creates its own `Replicated` database.

# Regression test for the `Replicated`-database side of
# https://github.com/ClickHouse/ClickHouse/issues/105644.
#
# Data lake engines reject a user-supplied `compression_method`, but only for a
# fresh definition, i.e. `LoadingStrictnessLevel::CREATE`. `SECONDARY_CREATE` is
# exempt because `DatabaseReplicated::recoverLostReplica` re-executes the stored
# `CREATE TABLE` text in that mode: a database created before the validation
# landed must still be recoverable on an upgraded replica.
#
# This test pins the other half of that contract: a *fresh* `CREATE TABLE` in a
# `Replicated` database is still rejected, because the initiator runs it as an
# initial query and therefore with `mode == CREATE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RDB="rdb_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${RDB}"
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${RDB}
    ENGINE = Replicated('/test/04695/${CLICKHOUSE_DATABASE}', 'shard1', 'replica1')
"

# The rejection fires while parsing the engine arguments, before any file
# access, so the path does not need to hold an Iceberg table.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${RDB}.t_lzma (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_rdb_lzma', 'Parquet', 'lzma')
" 2>&1 | grep -o -m1 "not supported by data lake engines"

# The table must not have been created.
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${RDB}.t_lzma"

# Without a `compression_method` the same statement is accepted.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${RDB}.t_default (c0 Int)
    ENGINE = IcebergLocal('${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_rdb_default', 'Parquet')
" --distributed_ddl_output_mode 'none'
${CLICKHOUSE_CLIENT} --query "EXISTS TABLE ${RDB}.t_default"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${RDB}"
