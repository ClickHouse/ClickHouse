#!/usr/bin/env bash
# Tags: no-fasttest, no-cpu-s390x
# no-fasttest -- compiled w/o datasketches

# Malformed states must raise `CORRUPTED_DATA`, not a logical exception.
# A shell test tolerates the client's extra stack trace, as in
# `04307_uniqTheta_corrupted_state_106259.sh`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Valid length prefix, invalid sketch preamble.
$CLICKHOUSE_CLIENT --query \
    "SELECT finalizeAggregation(CAST(unhex('08FFFFFFFFFFFFFFFF'), 'AggregateFunction(uniqApacheHLL, UInt64)'))" 2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK unknown type' || echo 'FAIL unknown type'

# The `RowBinary` payload is shorter than any valid HLL sketch.
printf '\x03\x03\x03\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary \
        --structure='x AggregateFunction(uniqApacheHLL, IPv6)' \
        --query='SELECT x FROM table' 2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK rowbinary' || echo 'FAIL rowbinary'
