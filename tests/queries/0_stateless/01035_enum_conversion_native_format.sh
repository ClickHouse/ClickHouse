#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query="drop table if exists enum_source;"
${CLICKHOUSE_CLIENT} --query="drop table if exists enum_buf;"

${CLICKHOUSE_CLIENT} --query="create table enum_source(e Enum8('a'=1)) engine = MergeTree order by tuple()"
${CLICKHOUSE_CLIENT} --query="insert into enum_source values ('a')"
${CLICKHOUSE_CLIENT} --query="create table enum_buf engine = Log as select * from enum_source;"
${CLICKHOUSE_CLIENT} --query="alter table enum_source modify column e Enum8('a'=1, 'b'=2);"

${CLICKHOUSE_CLIENT} --query="select * from enum_buf format Native" \
    | ${CLICKHOUSE_CLIENT} --query="insert into enum_source format Native"

${CLICKHOUSE_CLIENT} --query="select * from enum_source;"

${CLICKHOUSE_CLIENT} --query="drop table enum_source;"
${CLICKHOUSE_CLIENT} --query="drop table enum_buf;"
