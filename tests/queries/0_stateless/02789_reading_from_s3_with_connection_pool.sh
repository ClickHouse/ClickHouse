#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-distributed-cache, no-parallel-replicas

# no-fasttest -- test uses s3_dick
# no-parallel-replicas -- do not run url functions as StorageURLCluster

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS test_s3;

CREATE TABLE test_s3 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0;

INSERT INTO test_s3 SELECT number, number FROM numbers_mt(1);
"

# This (reusing connections from the pool) is not guaranteed to always happen,
# (due to random time difference between the queries and random activity in parallel)
# but should happen most of the time.

for _ in {0..9}
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

# LIMIT 1 here is important part
# processor StorageURLSource releases the HTTP session either when all data is fully read in the last generate call or at d-tor of the processor instance
# with LIMIT 1 the HTTP session is released at pipeline d-tor because not all the data is read from HTTP connection inside StorageURLSource processor
# this tests covers the case when profile events have to be gathered and logged to the query_log only after pipeline is destroyed

for _ in {0..9}
do
    query_id=$(${CLICKHOUSE_CLIENT} -q "
    SELECT queryID() FROM(
        SELECT *
        FROM url(
            'http://localhost:8123/?query=' || encodeURLComponent('select 1'),
            'LineAsString',
            's String')
        ) LIMIT 1 SETTINGS max_threads=1, http_make_head_request=0;
    ")

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

    RES=$(${CLICKHOUSE_CLIENT} -m --query "
    SELECT ProfileEvents['StorageConnectionsPreserved'] > 0
    FROM system.query_log
    WHERE type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_id='$query_id';
    ")

    [[ $RES -eq 1 ]] && echo "StorageConnectionsPreserved $RES" && break;
done
