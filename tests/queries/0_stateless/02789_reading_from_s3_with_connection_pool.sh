#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS test_s3;

CREATE TABLE test_s3 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0;

INSERT INTO test_s3 SELECT number, number FROM numbers_mt(1e7);
"

# This (reusing connections from the pool) is not guaranteed to always happen,
# (due to random time difference between the queries and random activity in parallel)
# but should happen most of the time.

while true
do
    query="SELECT a, b FROM test_s3"
    query_id=$(${CLICKHOUSE_CLIENT} --query "select queryID() from ($query) limit 1" 2>&1)
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"

    RES=$(${CLICKHOUSE_CLIENT} -m --query "
    SELECT ProfileEvents['DiskConnectionsPreserved'] > 0
    FROM system.query_log
    WHERE type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_id='$query_id';
    ")

    [[ $RES -eq 1 ]] && echo "$RES" && break;
done


# Test connection pool in ReadWriteBufferFromHTTP

while true
do
    query_id=$(${CLICKHOUSE_CLIENT} -q "
    create table mut (n int, m int, k int) engine=ReplicatedMergeTree('/test/02441/{database}/mut', '1') order by n;
    set insert_keeper_fault_injection_probability=0;
    insert into mut values (1, 2, 3), (10, 20, 30);

    system stop merges mut;
    alter table mut delete where n = 10;

    select queryID() from(
        -- a funny way to wait for a MUTATE_PART to be assigned
        select sleepEachRow(2) from url('http://localhost:8123/?param_tries={1..10}&query=' || encodeURLComponent(
            'select 1 where ''MUTATE_PART'' not in (select type from system.replication_queue where database=''' || currentDatabase() || ''' and table=''mut'')'
            ), 'LineAsString', 's String')
        -- queryID() will be returned for each row, since the query above doesn't return anything we need to return a fake row
        union all
        select 1
    ) limit 1 settings max_threads=1;
    " 2>&1)
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
    RES=$(${CLICKHOUSE_CLIENT} -m --query "
    SELECT ProfileEvents['StorageConnectionsPreserved'] > 0
    FROM system.query_log
    WHERE type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_id='$query_id';
    ")

    [[ $RES -eq 1 ]] && echo "$RES" && break;
done
