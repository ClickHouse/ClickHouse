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
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

    RES=$(${CLICKHOUSE_CLIENT} -m --query "
    SELECT ProfileEvents['DiskConnectionsPreserved'] > 0
    FROM system.query_log
    WHERE type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_id='$query_id';
    ")

    [[ $RES -eq 1 ]] && echo "DiskConnectionsPreserved $RES" && break;
done


# Test connection pool in ReadWriteBufferFromHTTP
# we do here `while` because it might be a bad luck that the session is not preserved due to hitting server limits
# if we change LIMIT 2 to LIMIT 1 then test would fail because
# query_log collects the metrics before pipeline is destroyed
# StorageURLSource releases the HTTP session either when all data is fully read or at d-tor
# with LIMIT 1 the HTTP session is released after query_log is written

while true
do
    query_id=$(${CLICKHOUSE_CLIENT} -q "
    SELECT queryID() FROM(
        SELECT sleepEachRow(2)
        FROM url(
            'http://localhost:8123/?query=' || encodeURLComponent('select 1'),
            'LineAsString',
            's String')
            -- queryID() will be returned for each row, since the query above doesn't return anything we need to return a fake row
        ) LIMIT 2 SETTINGS max_threads=1, http_make_head_request=0;
    ")

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

    RES=$(${CLICKHOUSE_CLIENT} -m --query "
    SELECT ProfileEvents['StorageConnectionsPreserved'] > 0
    FROM system.query_log
    WHERE type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_id='$query_id';
    ")

    [[ $RES -eq 1 ]] && echo "DiskConnStorageConnectionsPreservedectionsPreserved $RES" && break;
done
