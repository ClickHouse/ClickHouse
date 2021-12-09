#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_01280_1"

function optimize()
{
    for _ in {0..20}; do
        $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $1 FINAL SETTINGS optimize_throw_if_noop=1" 2>/dev/null && break
        sleep 0.3
    done
}

echo "ttl_01280_1"
$CLICKHOUSE_CLIENT -n --query "
create table ttl_01280_1 (a Int, b Int, x Int, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second delete where x % 10 == 0 and y > 5;
insert into ttl_01280_1 values (1, 1, 0, 4, now() + 10);
insert into ttl_01280_1 values (1, 1, 10, 6, now());
insert into ttl_01280_1 values (1, 2, 3, 7, now());
insert into ttl_01280_1 values (1, 3, 0, 5, now());
insert into ttl_01280_1 values (2, 1, 20, 1, now());
insert into ttl_01280_1 values (2, 1, 0, 1, now());
insert into ttl_01280_1 values (3, 1, 0, 8, now());"

sleep 2
optimize "ttl_01280_1"
$CLICKHOUSE_CLIENT --query "select a, b, x, y from ttl_01280_1 ORDER BY a, b, x, y"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_01280_2"

echo "ttl_01280_2"
$CLICKHOUSE_CLIENT -n --query "
create table ttl_01280_2 (a Int, b Int, x Array(Int32), y Double, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set x = minForEach(x), y = sum(y), d = max(d);
insert into ttl_01280_2 values (1, 1, array(0, 2, 3), 4, now() + 10);
insert into ttl_01280_2 values (1, 1, array(5, 4, 3), 6, now());
insert into ttl_01280_2 values (1, 1, array(5, 5, 1), 7, now());
insert into ttl_01280_2 values (1, 3, array(3, 0, 4), 5, now());
insert into ttl_01280_2 values (1, 3, array(1, 1, 2, 1), 9, now());
insert into ttl_01280_2 values (1, 3, array(3, 2, 1, 0), 3, now());
insert into ttl_01280_2 values (2, 1, array(3, 3, 3), 7, now());
insert into ttl_01280_2 values (2, 1, array(11, 1, 0, 3), 1, now());
insert into ttl_01280_2 values (3, 1, array(2, 4, 5), 8, now());"

sleep 2
optimize "ttl_01280_2"
$CLICKHOUSE_CLIENT --query "select a, b, x, y from ttl_01280_2 ORDER BY a, b, x, y"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_01280_3"

echo "ttl_01280_3"
$CLICKHOUSE_CLIENT -n --query "
create table ttl_01280_3 (a Int, b Int, x Int64, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a set b = min(b), x = argMax(x, d), y = argMax(y, d), d = max(d);
insert into ttl_01280_3 values (1, 1, 0, 4, now() + 10);
insert into ttl_01280_3 values (1, 1, 10, 6, now() + 1);
insert into ttl_01280_3 values (1, 2, 3, 7, now());
insert into ttl_01280_3 values (1, 3, 0, 5, now());
insert into ttl_01280_3 values (2, 1, 20, 1, now());
insert into ttl_01280_3 values (2, 1, 0, 3, now() + 1);
insert into ttl_01280_3 values (3, 1, 0, 3, now());
insert into ttl_01280_3 values (3, 2, 8, 2, now() + 1);
insert into ttl_01280_3 values (3, 5, 5, 8, now());"

sleep 2
optimize "ttl_01280_3"
$CLICKHOUSE_CLIENT --query "select a, b, x, y from ttl_01280_3 ORDER BY a, b, x, y"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_01280_4"

echo "ttl_01280_4"
$CLICKHOUSE_CLIENT -n --query "
create table ttl_01280_4 (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), -(a + b)) ttl d + interval 1 second group by toDate(d) set x = sum(x), y = max(y);
insert into ttl_01280_4 values (1, 1, 0, 4, now() + 10);
insert into ttl_01280_4 values (10, 2, 3, 3, now());
insert into ttl_01280_4 values (2, 10, 1, 7, now());
insert into ttl_01280_4 values (3, 3, 5, 2, now());
insert into ttl_01280_4 values (1, 5, 4, 9, now())"

sleep 2
optimize "ttl_01280_4"
$CLICKHOUSE_CLIENT --query "select x, y from ttl_01280_4 ORDER BY a, b, x, y"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_01280_5"

echo "ttl_01280_5"
$CLICKHOUSE_CLIENT -n --query "create table ttl_01280_5 (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), a, -b) ttl d + interval 1 second group by toDate(d), a set x = sum(x), b = argMax(b, -b);
insert into ttl_01280_5 values (1, 2, 3, 5, now());
insert into ttl_01280_5 values (2, 10, 1, 5, now());
insert into ttl_01280_5 values (2, 3, 5, 5, now());
insert into ttl_01280_5 values (1, 5, 4, 5, now());"

sleep 2
optimize "ttl_01280_5"
$CLICKHOUSE_CLIENT --query "select a, b, x, y from ttl_01280_5 ORDER BY a, b, x, y"

$CLICKHOUSE_CLIENT --query "drop table if exists ttl_01280_6"

echo "ttl_01280_6"
$CLICKHOUSE_CLIENT -n --query "
create table ttl_01280_6 (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), a, -b) ttl d + interval 1 second group by toDate(d), a;
insert into ttl_01280_6 values (1, 2, 3, 5, now());
insert into ttl_01280_6 values (2, 10, 3, 5, now());
insert into ttl_01280_6 values (2, 3, 3, 5, now());
insert into ttl_01280_6 values (1, 5, 3, 5, now())"

sleep 2
optimize "ttl_01280_6"
$CLICKHOUSE_CLIENT --query "select a, x, y from ttl_01280_6 ORDER BY a, b, x, y"

$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_01280_1"
$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_01280_2"
$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_01280_3"
$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_01280_4"
$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_01280_5"
$CLICKHOUSE_CLIENT -q "DROP TABLE ttl_01280_6"
