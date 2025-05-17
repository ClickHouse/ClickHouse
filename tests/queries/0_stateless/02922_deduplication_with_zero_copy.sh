#!/usr/bin/env bash
# Tags: long, no-replicated-database, no-fasttest, no-shared-merge-tree

set -e

CURDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -m -q "
drop table if exists r1;
drop table if exists r2;

create table r1 (n int)
    engine=ReplicatedMergeTree('/test/02922/{database}/table', '1')
    order by tuple()
    settings
        storage_policy='s3_cache',
        allow_remote_fs_zero_copy_replication=1;

create table r2 (n int)
    engine=ReplicatedMergeTree('/test/02922/{database}/table', '2')
    order by tuple()
    settings
        storage_policy='s3_cache',
        allow_remote_fs_zero_copy_replication=1;
"


function get_shared_locks()
{
  table_shared_id="$1"
  for part in $($CLICKHOUSE_KEEPER_CLIENT -q "ls '/clickhouse/zero_copy/zero_copy_s3/${table_shared_id}'")
  do
    for blob in $($CLICKHOUSE_KEEPER_CLIENT -q "ls '/clickhouse/zero_copy/zero_copy_s3/${table_shared_id}/${part}'")
    do
      for lock in $($CLICKHOUSE_KEEPER_CLIENT -q "ls '/clickhouse/zero_copy/zero_copy_s3/${table_shared_id}/${part}/${blob}'")
      do
        echo "/clickhouse/zero_copy/zero_copy_s3/${table_shared_id}/${part}/${blob}/${lock}"
      done
    done
  done
}


function filter_temporary_locks()
{
  while read -r lock
  do
    owner="$($CLICKHOUSE_KEEPER_CLIENT -q "get_stat '${lock}'" | grep 'ephemeralOwner' | sed 's/.*= //')"
    if [[ "${owner}" -eq "0" ]]
    then
      echo "${lock}"
    fi
  done
}


function insert_duplicates() {
  $CLICKHOUSE_CLIENT -q "insert into r1 values(1);" --allow_repeated_settings --send_logs_level="error"  &

  $CLICKHOUSE_CLIENT -q "insert into r2 values(1);" --allow_repeated_settings --send_logs_level="error"

  wait

  $CLICKHOUSE_CLIENT -m -q "
system sync replica r1;
system sync replica r2;
"

  count="$($CLICKHOUSE_CLIENT -q "select count() from r2;")"

  [[ "${count}" -eq "1" ]]
}

function loop()
{
  set -e

  table_shared_id="$1"

  local TIMELIMIT=$((SECONDS+TIMEOUT))
  while [ $SECONDS -lt "$TIMELIMIT" ]
  do
    while ! insert_duplicates
    do
       $CLICKHOUSE_CLIENT -m -q "
truncate table r1;
truncate table r2;
system sync replica r1;
system sync replica r2;
"
    done

    persistent_locks="$(get_shared_locks "${table_shared_id}" | filter_temporary_locks)"
    num="$(echo "${persistent_locks}" | wc -w)"

    if [[ "${num}" -ne "2" ]]
    then
      echo "${persistent_locks}"
      break
    fi
  done

}


table_shared_id="$($CLICKHOUSE_KEEPER_CLIENT -q "get '/test/02922/${CLICKHOUSE_DATABASE}/table/table_shared_id'")"

exit_code=0
TIMEOUT=40
loop "${table_shared_id}"


function list_keeper_nodes() {
  table_shared_id=$1

  echo "zero_copy:"
  $CLICKHOUSE_KEEPER_CLIENT -q "ls '/clickhouse/zero_copy/zero_copy_s3'" | grep -o "${table_shared_id}" | \
    sed "s/${table_shared_id}/<table_shared_id>/g" || :

  echo "tables:"
  $CLICKHOUSE_KEEPER_CLIENT -q "ls '/test/02922/${CLICKHOUSE_DATABASE}'" | grep -o "table" || :
}

list_keeper_nodes "${table_shared_id}"

$CLICKHOUSE_CLIENT -m -q "drop table r1;" --allow_repeated_settings --send_logs_level="error"  &
$CLICKHOUSE_CLIENT -m -q "drop table r2;" --allow_repeated_settings --send_logs_level="error"  &
wait

list_keeper_nodes "${table_shared_id}"
