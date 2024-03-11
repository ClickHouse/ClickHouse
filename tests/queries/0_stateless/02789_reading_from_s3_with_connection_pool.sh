#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -nm --query "
DROP TABLE IF EXISTS test_s3;

CREATE TABLE test_s3 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0;

INSERT INTO test_s3 SELECT number, number FROM numbers_mt(1e7);
"
query="SELECT a, b FROM test_s3"
query_id=$(${CLICKHOUSE_CLIENT} --query "select queryID() from ($query) limit 1" 2>&1)
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -nm --query "
WITH
    ProfileEvents['DiskConnectionsReset'] AS reset,
    ProfileEvents['DiskConnectionsPreserved'] AS preserved
SELECT preserved > reset
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query_id='$query_id';
"


# Test connection pool in ReadWriteBufferFromHTTP

query_id=$(${CLICKHOUSE_CLIENT} -nq "
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
${CLICKHOUSE_CLIENT} -nm --query "
SELECT ProfileEvents['StorageConnectionsPreserved'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query_id='$query_id';
"
