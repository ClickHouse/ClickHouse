#!/usr/bin/env bash
# Tags: no-fasttest, long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

table_structure="id UInt64"

for i in {1..250}; do
    table_structure+=", c$i String"
done

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_insert_mem;
    DROP TABLE IF EXISTS t_reference;

    CREATE TABLE t_insert_mem ($table_structure) ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
    CREATE TABLE t_reference ($table_structure) ENGINE = Log;

    SYSTEM STOP MERGES t_insert_mem;
"

filename="test_data_sparse_$CLICKHOUSE_DATABASE.json"

$CLICKHOUSE_CLIENT --query "
    INSERT INTO FUNCTION file('$filename', LineAsString)
    SELECT format('{{ \"id\": {}, \"c{}\": \"{}\" }}', number, number % 250, hex(number * 1000000)) FROM numbers(30000)
    SETTINGS engine_file_truncate_on_insert = 1;

    INSERT INTO FUNCTION s3(s3_conn, filename='$filename', format='LineAsString')
    SELECT * FROM file('$filename', LineAsString)
    SETTINGS s3_truncate_on_insert = 1;
"

for _ in {1..4}; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_reference SELECT * FROM file('$filename', JSONEachRow)"
done;

$CLICKHOUSE_CLIENT --enable_parsing_to_custom_serialization 1 --query "INSERT INTO t_insert_mem SELECT * FROM file('$filename', JSONEachRow)"
$CLICKHOUSE_CLIENT --enable_parsing_to_custom_serialization 1 --query "INSERT INTO t_insert_mem SELECT * FROM file('$filename', JSONEachRow)"
$CLICKHOUSE_CLIENT --enable_parsing_to_custom_serialization 1 --query "INSERT INTO t_insert_mem SELECT * FROM s3(s3_conn, filename='$filename', format='JSONEachRow')"
$CLICKHOUSE_CLIENT --query "SELECT * FROM file('$filename', LineAsString) FORMAT LineAsString" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_insert_mem+FORMAT+JSONEachRow&enable_parsing_to_custom_serialization=1" --data-binary @-

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_insert_mem;
    SELECT sum(sipHash64(*)) FROM t_insert_mem;
    SELECT sum(sipHash64(*)) FROM t_reference;

    SELECT serialization_kind, count() FROM system.parts_columns
    WHERE table = 't_insert_mem' AND database = '$CLICKHOUSE_DATABASE'
    GROUP BY serialization_kind ORDER BY serialization_kind;

    SYSTEM FLUSH LOGS;

    SELECT written_bytes <= 3000000 FROM system.query_log
    WHERE query LIKE 'INSERT INTO t_insert_mem%' AND current_database = '$CLICKHOUSE_DATABASE' AND type = 'QueryFinish'
    ORDER BY event_time_microseconds;

    DROP TABLE IF EXISTS t_insert_mem;
    DROP TABLE IF EXISTS t_reference;
"
