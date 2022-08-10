#!/usr/bin/env bash
# Tags: long

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

tmp_file=$(mktemp "$CURDIR/clickhouse.XXXXXX.csv")
trap 'rm $tmp_file' EXIT

# NOTE: this file should be huge enough, so that it is impossible to upload it
# in 0.15s, see timeout command below, this will ensure, that EOF will be
# received during creating a set from externally uploaded table.
#
# Previously code there wasn't ready for EOF, and you will get one of the
# following assertions:
#
#     - ./src/IO/ReadBuffer.h:58: bool DB::ReadBuffer::next(): Assertion `!hasPendingData()' failed.
#     - ./src/Server/HTTP/HTMLForm.cpp:245: bool DB::HTMLForm::MultipartReadBuffer::skipToNextBoundary(): Assertion `boundary_hit' failed.
#     - ./src/IO/LimitReadBuffer.cpp:17: virtual bool DB::LimitReadBuffer::nextImpl(): Assertion `position() >= in->position()' failed.
#
$CLICKHOUSE_CLIENT -q "SELECT toString(number) FROM numbers(10e6) FORMAT TSV" > "$tmp_file"

# NOTE: Just in case check w/ input_format_parallel_parsing and w/o
timeout 0.15s ${CLICKHOUSE_CURL} -sS -F "s=@$tmp_file;" "${CLICKHOUSE_URL}&s_structure=key+Int&query=SELECT+dummy+IN+s&input_format_parallel_parsing=true" -o /dev/null
echo $?
timeout 0.15s ${CLICKHOUSE_CURL} -sS -F "s=@$tmp_file;" "${CLICKHOUSE_URL}&s_structure=key+Int&query=SELECT+dummy+IN+s&input_format_parallel_parsing=false" -o /dev/null
echo $?

# wait until the query above will start,
# so that clickhouse_test_wait_queries will see them.
sleep 5

clickhouse_test_wait_queries 60
