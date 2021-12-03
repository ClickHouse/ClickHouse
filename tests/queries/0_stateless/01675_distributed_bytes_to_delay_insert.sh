#!/usr/bin/env bash

# NOTE: $SECONDS accuracy is second, so we need some delta, hence -1 in time conditions.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

max_delay_to_insert=5

${CLICKHOUSE_CLIENT} -nq "
drop table if exists dist_01675;
drop table if exists data_01675;
"

${CLICKHOUSE_CLIENT} -nq "
create table data_01675 (key Int) engine=Null();
create table dist_01675 (key Int) engine=Distributed(test_shard_localhost, currentDatabase(), data_01675) settings bytes_to_delay_insert=1, max_delay_to_insert=$max_delay_to_insert;
system stop distributed sends dist_01675;
"

#
# Case 1: max_delay_to_insert will throw.
#
echo "max_delay_to_insert will throw"

start_seconds=$SECONDS
${CLICKHOUSE_CLIENT} --testmode -nq "
-- first batch is always OK, since there is no pending bytes yet
insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0;
-- second will fail, because of bytes_to_delay_insert=1 and max_delay_to_insert=5,
-- while distributed sends is stopped.
--
-- (previous block definitelly takes more, since it has header)
insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0; -- { serverError 574 }
system flush distributed dist_01675;
"
end_seconds=$SECONDS

if (( (end_seconds-start_seconds)<(max_delay_to_insert-1) )); then
    echo "max_delay_to_insert was not satisfied ($end_seconds-$start_seconds)"
fi

#
# Case 2: max_delay_to_insert will finally finished.
#
echo "max_delay_to_insert will succeed"

max_delay_to_insert=10
${CLICKHOUSE_CLIENT} -nq "
drop table dist_01675;
create table dist_01675 (key Int) engine=Distributed(test_shard_localhost, currentDatabase(), data_01675) settings bytes_to_delay_insert=1, max_delay_to_insert=$max_delay_to_insert;
system stop distributed sends dist_01675;
"

flush_delay=4
function flush_distributed_worker()
{
    sleep $flush_delay
    ${CLICKHOUSE_CLIENT} -q "system flush distributed dist_01675"
    echo flushed
}
flush_distributed_worker &

start_seconds=$SECONDS
${CLICKHOUSE_CLIENT} --testmode -nq "
-- first batch is always OK, since there is no pending bytes yet
insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0;
-- second will succcedd, due to SYSTEM FLUSH DISTRIBUTED in background.
insert into dist_01675 select * from numbers(1) settings prefer_localhost_replica=0;
"
end_seconds=$SECONDS
wait

if (( (end_seconds-start_seconds)<(flush_delay-1) )); then
    echo "max_delay_to_insert was not wait flush_delay ($end_seconds-$start_seconds)"
fi
if (( (end_seconds-start_seconds)>=(max_delay_to_insert-1) )); then
    echo "max_delay_to_insert was overcommited ($end_seconds-$start_seconds)"
fi


${CLICKHOUSE_CLIENT} -nq "
drop table dist_01675;
drop table data_01675;
"
