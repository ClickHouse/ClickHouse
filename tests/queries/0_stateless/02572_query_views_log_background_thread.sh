#!/usr/bin/env bash

# INSERT buffer_02572 -> data_02572 -> copy_02572
#                                   ^^
#                             push to system.query_views_log

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --ignore-error --query "drop table if exists buffer_02572;
    drop table if exists data_02572; drop table if exists copy_02572; drop table if exists mv_02572;"

${CLICKHOUSE_CLIENT} --query="create table copy_02572 (key Int) engine=Memory();"
${CLICKHOUSE_CLIENT} --query="create table data_02572 (key Int) engine=Memory();"
${CLICKHOUSE_CLIENT} --query="create table buffer_02572 (key Int) engine=Buffer(currentDatabase(), data_02572, 1, 8, 8, 1, 1e9, 1, 1e9);"
${CLICKHOUSE_CLIENT} --query="create materialized view mv_02572 to copy_02572 as select * from data_02572;"

start=$(date +%s)
${CLICKHOUSE_CLIENT} --query="insert into buffer_02572 values (1);"

if [ $(( $(date +%s) - start )) -gt 6 ]; then  # clickhouse test cluster is overloaded, will skip
    # ensure that the flush was not direct
    ${CLICKHOUSE_CLIENT} --ignore-error --query "select * from data_02572; select * from copy_02572;"
fi

# we cannot use OPTIMIZE, this will attach query context, so let's wait
for _ in {1..100}; do
    $CLICKHOUSE_CLIENT -q "select * from data_02572;" | grep -q "1" && echo 'OK' && break
    sleep 0.5
done


${CLICKHOUSE_CLIENT} --ignore-error --query "select * from data_02572; select * from copy_02572;"

${CLICKHOUSE_CLIENT} --query="system flush logs;"
${CLICKHOUSE_CLIENT} --query="select count() > 0, lower(status::String), errorCodeToName(exception_code)
    from system.query_views_log where
    view_name = concatWithSeparator('.', currentDatabase(), 'mv_02572') and
    view_target = concatWithSeparator('.', currentDatabase(), 'copy_02572')
    group by 2, 3;"
