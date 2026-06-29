#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest -- compiled w/o datasketches

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106259
#
# Deserializing a malformed `AggregateFunction(uniqTheta, ...)` state used to
# trigger a `std::out_of_range` / `std::invalid_argument` from the
# `datasketches` library. Those are not `DB::Exception`, so they escaped
# `SerializationAggregateFunction`'s `catch (...)` block and were treated as
# `LOGICAL_ERROR` (code 1001) at the top level, aborting the process via
# `abortOnFailedAssertion`. The fix in `ThetaSketchData::read` translates the
# bare `std::exception` from the third-party library into
# `DB::Exception(CORRUPTED_DATA)` so the bad input is rejected cleanly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# RowBinary path. The 24 bytes encode one row: first byte 0x03 is a varint
# saying the sketch payload is 3 bytes long, which is shorter than the Theta
# sketch minimum, so `check_memory_size` throws `std::out_of_range` from inside
# `datasketches::compact_theta_sketch::deserialize`. Before the fix this
# aborted with SIGABRT (exit 134); after the fix it must produce
# `CORRUPTED_DATA`.
printf '\x03\x03\x03\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    | $CLICKHOUSE_LOCAL --input-format=RowBinary \
        --structure='x AggregateFunction(uniqTheta, IPv6)' \
        --query='SELECT x FROM table' 2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK rowbinary' || echo 'FAIL rowbinary'

# Query-parameter path: deserialised via `deserializeTextEscaped` rather than
# `deserializeBinary`, but the eventual call into `ThetaSketchData::read` is
# the same. This is the original fuzzer-found code path.
$CLICKHOUSE_LOCAL \
    --param_p=$'\x03\x03\x03\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
    --query='SELECT count() FROM numbers(1) WHERE {p:AggregateFunction(uniqTheta, IPv6)} IS NOT NULL' \
    2>&1 \
    | grep -q -F 'CORRUPTED_DATA' && echo 'OK param' || echo 'FAIL param'
