#!/usr/bin/env bash
# Tags: no-parallel, no-ordinary-database
#       ^^^^^^^^^^^
# Since the underlying view may disappears while flushing log, and leads to:
#
#     DB::Exception: Table test_x449vo..inner_id.9c14fb82-e6b1-4d1a-85a6-935c3a2a2029 is dropped. (TABLE_IS_DROPPED)
#

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# regression test for columns TTLs
# note, that this should be written in .sh since we need $CLICKHOUSE_DATABASE
# not 'default' to catch text_log

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists ttl_02262;
    drop table if exists this_text_log;

    create table ttl_02262 (date Date, key Int, value String TTL date + interval 1 month) engine=MergeTree order by key;
    insert into ttl_02262 values ('2010-01-01', 2010, 'foo');
    optimize table ttl_02262 final;

    detach table ttl_02262;
    attach table ttl_02262;

    -- create system.text_log
    system flush logs;
"

ttl_02262_uuid=$($CLICKHOUSE_CLIENT -q "select uuid from system.tables where database = '$CLICKHOUSE_DATABASE' and name = 'ttl_02262'")

$CLICKHOUSE_CLIENT -nm -q "
    -- OPTIMIZE TABLE x FINAL will be done in background
    -- attach to it's log, via table UUID in query_id (see merger/mutator code).
    create materialized view this_text_log engine=Memory() as
    select * from system.text_log where query_id like '%${ttl_02262_uuid}%';

    optimize table ttl_02262 final;
    system flush logs;
    -- If TTL will be applied again (during OPTIMIZE TABLE FINAL) it will produce the following message:
    --
    --     Some TTL values were not calculated for part 201701_487_641_3. Will calculate them forcefully during merge.
    --
    -- Let's ensure that this is not happen anymore:
    select count()>0, countIf(message LIKE '%TTL%') from this_text_log;

    drop table ttl_02262;
    drop table this_text_log;
"
