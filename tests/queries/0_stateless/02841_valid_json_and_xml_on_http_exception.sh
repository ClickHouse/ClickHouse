#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


CH_URL_BASE="$CLICKHOUSE_URL&http_write_exception_in_output_format=1&enable_analyzer=0"


for http_wait_end_of_query in 0 1
do
    echo "http_wait_end_of_query=$http_wait_end_of_query"
    CH_URL="$CH_URL_BASE&http_wait_end_of_query=$http_wait_end_of_query"

    echo "One block"
    for parallel in 0 1
    do
        echo "Parallel formatting: $parallel"
        for format in JSON JSONEachRow JSONCompact JSONCompactEachRow JSONObjectEachRow XML
        do
            echo $format
            ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select number, throwIf(number > 3) as res from numbers(10) format $format settings output_format_parallel_formatting=$parallel" | sed "s/(version .*)//" | sed "s/DB::Exception//"
        done
    done

    echo "Several blocks"
    echo "Without parallel formatting"
    for format in JSON JSONEachRow JSONCompact JSONCompactEachRow JSONObjectEachRow XML
    do
        echo $format
            ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select number, throwIf(number > 3) as res from system.numbers format $format settings max_block_size=1, output_format_parallel_formatting=0" | sed "s/(version .*)//" | sed "s/DB::Exception//"
    done

    echo "With parallel formatting"
    for format in JSON JSONCompact JSONObjectEachRow
    do
        echo $format
        ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select number, throwIf(number > 3) as res from system.numbers format $format settings max_block_size=1, output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"
    done

    for format in JSONEachRow JSONCompactEachRow
    do
        echo $format
        ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select number, throwIf(number > 3) as res from system.numbers format $format settings max_block_size=1, output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=LineAsString -q "select min(isValidJSON(line)) from table"
    done

    echo "Formatting error"
    $CLICKHOUSE_CLIENT -q "drop table if exists test_02841"
    $CLICKHOUSE_CLIENT -q "create table test_02841 (x UInt32, s String, y Enum('a' = 1)) engine=MergeTree order by x"
    $CLICKHOUSE_CLIENT -q "system stop merges test_02841"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (1, 'str1', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (2, 'str2', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (3, 'str3', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (5, 'str5', 99)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (6, 'str6', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (7, 'str7', 1)"

    echo "Without parallel formatting"
    for format in JSON JSONEachRow JSONCompact JSONCompactEachRow JSONObjectEachRow XML
    do
        echo $format
        ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 order by x format $format settings output_format_parallel_formatting=0" | sed "s/(version .*)//" | sed "s/DB::Exception//"
    done

    echo "With parallel formatting"
    for format in JSON JSONCompact JSONObjectEachRow
    do
        echo $format
        ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format $format settings output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"
    done

    for format in JSONEachRow JSONCompactEachRow
    do
        echo $format
        ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format $format settings output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=LineAsString -q "select min(isValidJSON(line)) from table"
    done


    echo "Test 1"
    $CLICKHOUSE_CLIENT -q "truncate table test_02841"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select 1, repeat('aaaaa', 1000000), 1"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select 2, repeat('aaaaa', 1000000), 99"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select 3, repeat('aaaaa', 1000000), 1"

    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format JSON settings output_format_parallel_formatting=0" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"
    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format JSON settings output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"


    echo "Test 2"
    $CLICKHOUSE_CLIENT -q "truncate table test_02841"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (1, 'str1', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (2, 'str2', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select number, 'str_numbers_1', 1 from numbers(10000)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (3, 'str4', 99)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (4, 'str5', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select number, 'str_numbers_2', 1 from numbers(10000)"

    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format JSON settings output_format_parallel_formatting=0" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"
    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format JSON settings output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"

    echo "Test 3"
    $CLICKHOUSE_CLIENT -q "truncate table test_02841"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (1, 'str1', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (2, 'str2', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select number, 'str_numbers_1', number > 9000 ? 99 : 1 from numbers(10000)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (3, 'str4', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 values (4, 'str5', 1)"
    $CLICKHOUSE_CLIENT -q "insert into test_02841 select number, 'str_numbers_2', 1 from numbers(10000)"

    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format JSON settings output_format_parallel_formatting=0" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"
    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "select * from test_02841 format JSON settings output_format_parallel_formatting=1" | $CLICKHOUSE_LOCAL --input-format=JSONAsString -q "select isValidJSON(json) from table"

    $CLICKHOUSE_CLIENT -q "drop table test_02841"

done
