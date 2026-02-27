#!/usr/bin/env bash
# Tags: no-fasttest, long, no-azure-blob-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

table_structure="id UInt64"

for i in {1..250}; do
    table_structure+=", c$i String"
done

# Redirect server logs to /dev/null to suppress sporadic `ConnectionGroup: Too many active sessions` warnings
# that appear in stderr when many S3 connections are opened across parallel tests.
MY_CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_parsing_to_custom_serialization 1 --parallel_replicas_for_cluster_engines 0 --server_logs_file=/dev/null"

$MY_CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_insert_mem;
    DROP TABLE IF EXISTS t_reference;

    CREATE TABLE t_insert_mem ($table_structure)
    ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, index_granularity = 8192, index_granularity_bytes = '10M';

    CREATE TABLE t_reference ($table_structure) ENGINE = Log;

    SYSTEM STOP MERGES t_insert_mem;
"

filename="test_data_sparse_$CLICKHOUSE_DATABASE.json"

# 10000 rows with 250 String columns. Each row sets only one column (c{number % 250}),
# so each column has ~40 non-default values out of 10000 (99.6% defaults, above the 90% sparse threshold).
# Without sparse serialization, each INSERT writes ~20MB (250 columns × 10000 rows × 8 bytes offset).
# With sparse serialization, only non-default values are stored, bringing it well under the 10MB threshold.
$MY_CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION file('$filename', LineAsString)
    SELECT format('{{ \"id\": {}, \"c{}\": \"{}\" }}', number, number % 250, hex(number * 1000000)) FROM numbers(10000)
    SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION s3(s3_conn, filename='$filename', format='LineAsString')
    SELECT * FROM file('$filename', LineAsString)
    SETTINGS s3_truncate_on_insert = 1;
"

for _ in {1..4}; do
    $MY_CLICKHOUSE_CLIENT --query "INSERT INTO t_reference SELECT * FROM file('$filename', JSONEachRow)"
done;

$MY_CLICKHOUSE_CLIENT --query "INSERT INTO t_insert_mem SELECT * FROM file('$filename', JSONEachRow)"
$MY_CLICKHOUSE_CLIENT --query "INSERT INTO t_insert_mem SELECT * FROM file('$filename', JSONEachRow)"

$MY_CLICKHOUSE_CLIENT --query "DETACH TABLE t_insert_mem"
$MY_CLICKHOUSE_CLIENT --query "ATTACH TABLE t_insert_mem"

$MY_CLICKHOUSE_CLIENT --query "INSERT INTO t_insert_mem SELECT * FROM s3(s3_conn, filename='$filename', format='JSONEachRow')"
$MY_CLICKHOUSE_CLIENT --query "SELECT * FROM file('$filename', LineAsString) FORMAT LineAsString" | ${CLICKHOUSE_CURL} -sS --max-time 240 "${CLICKHOUSE_URL}&query=INSERT+INTO+t_insert_mem+FORMAT+JSONEachRow&enable_parsing_to_custom_serialization=1" --data-binary @-

$MY_CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_insert_mem;
    SELECT (SELECT sum(sipHash64(*)) FROM t_insert_mem) = (SELECT sum(sipHash64(*)) FROM t_reference);

    SELECT serialization_kind, count() FROM system.parts_columns
    WHERE table = 't_insert_mem' AND database = '$CLICKHOUSE_DATABASE'
    GROUP BY serialization_kind ORDER BY serialization_kind;
"

# Wait for all INSERT query_log entries to appear.
# There is a race between HTTP response being sent and the query_log entry being written.
for _ in $(seq 1 60); do
    $MY_CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    count=$($MY_CLICKHOUSE_CLIENT --query "SELECT count() FROM system.query_log WHERE query LIKE 'INSERT INTO t_insert_mem%' AND current_database = '$CLICKHOUSE_DATABASE' AND type = 'QueryFinish'")
    [ "$count" -ge 4 ] && break
    sleep 0.5
done

$MY_CLICKHOUSE_CLIENT --query "
    SELECT written_bytes <= 10000000 FROM system.query_log
    WHERE query LIKE 'INSERT INTO t_insert_mem%' AND current_database = '$CLICKHOUSE_DATABASE' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds;

    DROP TABLE IF EXISTS t_insert_mem;
    DROP TABLE IF EXISTS t_reference;
"
