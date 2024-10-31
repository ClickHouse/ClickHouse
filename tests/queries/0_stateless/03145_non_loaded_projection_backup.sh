#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
drop table if exists tp_1;
create table tp_1 (x Int32, y Int32, projection p (select x, y order by x)) engine = MergeTree order by y partition by intDiv(y, 100) settings max_parts_to_merge_at_once=1;
insert into tp_1 select number, number from numbers(3);

set mutations_sync = 2;

alter table tp_1 add projection pp (select x, count() group by x);
insert into tp_1 select number, number from numbers(4);
select count() from tp_1;

-- Here we have a part with written projection pp
alter table tp_1 detach partition '0';
-- Move part to detached
alter table tp_1 clear projection pp;
-- Remove projection from table metadata
alter table tp_1 drop projection pp;
-- Now, we don't load projection pp for attached part, but it is written on disk
alter table tp_1 attach partition '0';
"

$CLICKHOUSE_CLIENT -m -q "
set send_logs_level='fatal';
check table tp_1 settings check_query_single_value_result = 0;" | grep -o "Found unexpected projection directories: pp.proj"

backup_id="$CLICKHOUSE_TEST_UNIQUE_NAME"
$CLICKHOUSE_CLIENT -q "
backup table tp_1 to Disk('backups', '$backup_id');
" | grep -o "BACKUP_CREATED"

$CLICKHOUSE_CLIENT -m -q "
set send_logs_level='fatal';
drop table tp_1;
restore table tp_1 from Disk('backups', '$backup_id');
" | grep -o "RESTORED"

$CLICKHOUSE_CLIENT -q "select count() from tp_1;"
$CLICKHOUSE_CLIENT -m -q "
set send_logs_level='fatal';
check table tp_1 settings check_query_single_value_result = 0;" | grep -o "Found unexpected projection directories: pp.proj"
$CLICKHOUSE_CLIENT -m -q "
set send_logs_level='fatal';
check table tp_1"
$CLICKHOUSE_CLIENT -m -q "
set send_logs_level='fatal';
drop table tp_1"
