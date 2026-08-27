#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eu

$CLICKHOUSE_CLIENT -q "
drop table if exists src;
drop table if exists dst_remote;
drop table if exists dst;
drop table if exists mv;
"

$CLICKHOUSE_CLIENT -q "
create table src (key Int, value Int)
engine=MergeTree()
ORDER BY key;

create table dst_remote (key Int, value Int)
engine=MergeTree()
ORDER BY key;

create table dst (key Int, value Int)
engine=URL('${CLICKHOUSE_URL}&async_insert=0&query=insert+into+dst_remote+format+TSV', TSV);

create materialized view mv to dst
as select * from src where value > 0;
"

$CLICKHOUSE_CLIENT --async_insert=0 -q "
insert into src values (1, 0), (2, 1), (3, 0), (4, 1);

insert into src values (10, 0), (20, 0), (30, 0), (40, 0);

insert into src values (100, 1), (200, 1), (300, 1), (400, 1);
"

$CLICKHOUSE_CLIENT -q "
select 'dst_remote', * from dst_remote order by ALL;
"

$CLICKHOUSE_CLIENT -q "
system flush logs query_log;

select
    query,
    written_rows,
    ProfileEvents['ReadWriteBufferFromHTTPRequestsSent'] + ProfileEvents['WriteBufferFromHTTPRequestsSent'] as HTTPRequests,
    ProfileEvents['StorageConnectionsCreated'] + ProfileEvents['StorageConnectionsReused'] as HTTPConnections
from system.query_log
where
    current_database = currentDatabase() and
    type = 'QueryFinish' and
    query_kind = 'Insert'
order by ALL
format Vertical;
"
