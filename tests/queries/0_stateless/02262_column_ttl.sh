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

$CLICKHOUSE_CLIENT -m -q "
    drop table if exists ttl_02262;
    drop table if exists this_text_log;

    create table ttl_02262 (date Date, key Int, value String TTL date + interval 1 month) engine=MergeTree order by key;
    insert into ttl_02262 values ('2010-01-01', 2010, 'foo');
    optimize table ttl_02262 final;

    detach table ttl_02262;
    attach table ttl_02262;

    -- create system.text_log
    system flush logs text_log;
"

$CLICKHOUSE_CLIENT -m -q "
    -- OPTIMIZE TABLE x FINAL will be done in background


    optimize table ttl_02262 final;

    system flush logs text_log, query_log;
    -- If TTL will be applied again (during OPTIMIZE TABLE FINAL) it will produce the following message:
    --
    --     Some TTL values were not calculated for part 201701_487_641_3. Will calculate them forcefully during merge.
    --
    -- Let's ensure that this is not happen anymore:

    WITH
        (
            SELECT query_id FROM system.query_log where has(databases, currentDatabase()) AND has(tables, currentDatabase() || '.ttl_02262')
            AND type = 'QueryFinish' AND query LIKE '%optimize table ttl_02262 final%' LIMIT 1
        ) AS optimize_qid
    SELECT
        count()>0, countIf(message LIKE '%TTL%')
    FROM system.text_log
    WHERE ((query_id = optimize_qid) OR (query_id = currentDatabase() || '.t_ind_merge_1::all_1_2_1'));

    drop table ttl_02262;
"
