#!/usr/bin/env bash
# Tags: no-fasttest, no-debug, no-asan, no-tsan, no-msan, no-ubsan

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$CUR_DIR/scripts_udf

$CLICKHOUSE_LOCAL -q 'create table exe_input_test
(
    `d` DateTime CODEC(Delta(4), ZSTD(1)),
    `c` Int32 CODEC(ZSTD(1)),
    `k` Int64 CODEC(ZSTD(1)),
    `v` Nullable(Float64) CODEC(ZSTD(1))
)
engine = ReplacingMergeTree
partition by toDate(d)
order by (d, k);' -q "insert into exe_input_test
select
  date_add(minute,ds.number*15,toDateTime('2024-01-01 00:00:00'))
  , 1 as c
  , os.number as k
  , randNormal(100,5) as v
from numbers_mt(250000) as os, numbers_mt(700) as ds;" -q "
select *
from executable(
  'to_dev_null.sh'
  ,RowBinary
  ,'value String'
  ,(
    select c,k,date_diff('mi',toDateTime('2024-01-14 00:00:00'),d) as o,v
    from exe_input_test
    order by c,k,o desc
    settings max_threads=4
  )
  , settings command_read_timeout=600000,command_write_timeout=600000
)
settings max_threads=4, max_execution_time = 60;" -- --user_scripts_path=$SCRIPTS_DIR 
