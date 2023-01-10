#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./merges.lib
. "$CURDIR"/merges.lib
set -e

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

# Column TTL works only with wide parts, because it's very expensive to apply it for compact parts

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (d DateTime, a Int ttl d + interval 1 second, b Int ttl d + interval 1 second) engine = MergeTree order by tuple() partition by toMinute(d) settings min_bytes_for_wide_part = 0;
insert into ttl_00933_1 values (now(), 1, 2);
insert into ttl_00933_1 values (now(), 3, 4);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select a, b from ttl_00933_1;"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (d DateTime, a Int, b Int)
    engine = MergeTree order by toDate(d) partition by tuple() ttl d + interval 1 second
    settings remove_empty_parts = 0;
insert into ttl_00933_1 values (now(), 1, 2);
insert into ttl_00933_1 values (now(), 3, 4);
insert into ttl_00933_1 values (now() + 1000, 5, 6);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;" # check ttl merge for part with both expired and unexpired values
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select a, b from ttl_00933_1;"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (d DateTime, a Int ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d) settings min_bytes_for_wide_part = 0;
insert into ttl_00933_1 values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl_00933_1 values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl_00933_1 values (toDateTime('2000-10-10 00:00:00'), 3);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1 order by d;"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (d DateTime, a Int)
    engine = MergeTree order by tuple() partition by tuple() ttl d + interval 1 day
    settings remove_empty_parts = 0;
insert into ttl_00933_1 values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl_00933_1 values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl_00933_1 values (toDateTime('2100-10-10 00:00:00'), 3);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1 order by d;"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (d Date, a Int)
    engine = MergeTree order by a partition by toDayOfMonth(d) ttl d + interval 1 day
    settings remove_empty_parts = 0;
insert into ttl_00933_1 values (toDate('2000-10-10'), 1);
insert into ttl_00933_1 values (toDate('2100-10-10'), 2);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1 order by d;"

# const DateTime TTL positive
$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (b Int, a Int ttl now()-1000) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create table ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1;"

# const DateTime TTL negative
$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (b Int, a Int ttl now()+1000) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create table ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1;"

# const Date TTL positive
$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (b Int, a Int ttl today()-1) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create table ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1;"

#const Date TTL negative
$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --multiquery --query "
create table ttl_00933_1 (b Int, a Int ttl today()+1) engine = MergeTree order by tuple() partition by tuple() settings min_bytes_for_wide_part = 0;
show create table ttl_00933_1;
insert into ttl_00933_1 values (1, 1);
"
$CLICKHOUSE_CLIENT --query "optimize table ttl_00933_1 final;"
wait_for_merges_done ttl_00933_1
$CLICKHOUSE_CLIENT --query "select * from ttl_00933_1;"

$CLICKHOUSE_CLIENT --query "set send_logs_level = 'fatal';"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"

$CLICKHOUSE_CLIENT --query "create table ttl_00933_1 (d DateTime ttl d) engine = MergeTree order by tuple() partition by toSecond(d);" 2>&1 | grep -o ILLEGAL_COLUMN | uniq || echo "expected error ILLEGAL_COLUMN"
$CLICKHOUSE_CLIENT --query "create table ttl_00933_1 (d DateTime, a Int ttl d) engine = MergeTree order by a partition by toSecond(d);" 2>&1 | grep -o ILLEGAL_COLUMN | uniq || echo "expected error ILLEGAL_COLUMN"
$CLICKHOUSE_CLIENT --query "create table ttl_00933_1 (d DateTime, a Int ttl 2 + 2) engine = MergeTree order by tuple() partition by toSecond(d);" 2>&1 | grep -o BAD_TTL_EXPRESSION | uniq || echo "expected error BAD_TTL_EXPRESSION"
$CLICKHOUSE_CLIENT --query "create table ttl_00933_1 (d DateTime, a Int ttl d - d) engine = MergeTree order by tuple() partition by toSecond(d);" 2>&1 | grep -o BAD_TTL_EXPRESSION | uniq || echo "expected error BAD_TTL_EXPRESSION"

$CLICKHOUSE_CLIENT --query "create table ttl_00933_1 (d DateTime, a Int  ttl d + interval 1 day) engine = Log;" 2>&1 | grep -o BAD_ARGUMENTS | uniq || echo "expected error BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT --query "create table ttl_00933_1 (d DateTime, a Int) engine = Log ttl d + interval 1 day;" 2>&1 | grep -o BAD_ARGUMENTS | uniq || echo "expected error BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_00933_1;"
