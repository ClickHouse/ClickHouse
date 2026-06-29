#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest -- compiled w/o datasketches

# Deserializing a malformed `AggregateFunction(quantileReq, ...)` state must not abort the
# process. The `datasketches` REQ sketch throws a bare `std::out_of_range` / `std::invalid_argument`
# on bad input; those are not `DB::Exception`, so without translation they escape
# `SerializationAggregateFunction`'s `catch (...)`, reach the top level as `LOGICAL_ERROR`
# (code 1001) and abort via `abortOnFailedAssertion`. `QuantileReq::deserialize` translates the
# bare `std::exception` into `DB::Exception(CORRUPTED_DATA)` so the bad input is rejected cleanly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RowBinary path. First byte 0x03 is a varint saying the sketch payload is 3 bytes long, which is
# shorter than the REQ sketch preamble (8 bytes), so `datasketches::req_sketch::deserialize` throws.
# Before the translation this aborted with SIGABRT (exit 134); now it must produce CORRUPTED_DATA.
printf '\x03\x01\x02\x03' \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary \
        --structure='x AggregateFunction(quantileReq(100, 0.5), UInt64)' \
        --query='SELECT x FROM table' 2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK rowbinary' || echo 'FAIL rowbinary'

# Query-parameter path: deserialised via `deserializeTextEscaped` rather than `deserializeBinary`,
# but the eventual call into `QuantileReq::deserialize` is the same.
$CLICKHOUSE_LOCAL \
    --param_p=$'\x03\x01\x02\x03' \
    --query='SELECT count() FROM numbers(1) WHERE {p:AggregateFunction(quantileReq(100, 0.5), UInt64)} IS NOT NULL' \
    2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK param' || echo 'FAIL param'
