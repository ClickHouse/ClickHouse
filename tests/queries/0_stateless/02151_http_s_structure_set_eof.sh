#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

tmp_file=$(mktemp "$CURDIR/clickhouse.XXXXXX.csv")
trap 'rm $tmp_file' EXIT

# This test verifies that the server handles EOF correctly when an HTTP
# multipart upload is interrupted mid-stream (the upload is killed by the
# parent shell after 0.15s).
#
# Previously the code wasn't ready for EOF, and you will get one of the
# following exceptions:
#
#     - ./src/IO/ReadBuffer.h:58: bool DB::ReadBuffer::next(): Assertion `!hasPendingData()' failed.
#     - ./src/Server/HTTP/HTMLForm.cpp:245: bool DB::HTMLForm::MultipartReadBuffer::skipToNextBoundary(): Assertion `boundary_hit' failed.
#     - ./src/IO/LimitReadBuffer.cpp:17: virtual bool DB::LimitReadBuffer::nextImpl(): Assertion `position() >= in->position()' failed.
#
# `curl --limit-rate` throttles the upload to a known rate, so the kill at
# 0.15s reliably arrives mid-upload regardless of CPU speed or network
# bandwidth. Without it, on a fast (release-style, e.g. `*_binary`) build
# over loopback the entire 78 MB upload can complete in under 0.15s, and
# the `Error: completed early` branch fires — leading to a flaky
# `+Error: completed early` diff against the reference output.
$CLICKHOUSE_CLIENT -q "SELECT toString(number) FROM numbers(10e6) FORMAT TSV" > "$tmp_file"

_timeout() {
    echo Run
    (
        ${CLICKHOUSE_CURL} --limit-rate 100k -sS -F "s=@$tmp_file;" "$1" -o /dev/null 2>/dev/null
        echo Error: completed early
    ) &
    local pid=$!
    sleep 0.15
    kill $pid 2>/dev/null
    wait $pid 2>/dev/null
    kill -- -$pid 2>/dev/null ||:
}

# NOTE: Just in case check w/ input_format_parallel_parsing and w/o
_timeout "${CLICKHOUSE_URL}&s_structure=key+Int&query=SELECT+dummy+IN+s&input_format_parallel_parsing=true"
_timeout "${CLICKHOUSE_URL}&s_structure=key+Int&query=SELECT+dummy+IN+s&input_format_parallel_parsing=false"

