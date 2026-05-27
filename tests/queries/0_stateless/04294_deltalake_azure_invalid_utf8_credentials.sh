#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: depends on delta-kernel-rs (not built in fast test)
# Tag no-msan: delta-kernel-rs is not built with MSan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104509
# and https://github.com/ClickHouse/ClickHouse/issues/105895.
#
# A binary SAS token (e.g. raw `MD5(...)` bytes) used to abort the server because
# the `delta-kernel-rs` FFI panicked on invalid UTF-8 inside `set_builder_option`,
# crossing the `extern "C"` boundary and triggering `panic_in_cleanup`. The C++
# wrapper now validates the bytes and rejects the input with `BAD_ARGUMENTS`
# instead of letting the panic propagate.
#
# Written as a `.sh` test (not `.sql` with `{ serverError BAD_ARGUMENTS }`) so
# that Bugfix validation can record a clean test-level result against the
# unfixed master binary. The unfixed code aborts the process, which prevents
# `clickhouse-test` from registering a `.sql` `{ serverError }` mismatch as a
# test `FAIL`. With `clickhouse-local` + grep, the unfixed binary yields an
# empty stdout (process crash, no `BAD_ARGUMENTS` text) -> mismatch with the
# reference -> clean `FAIL`; the fixed binary prints the `BAD_ARGUMENTS`
# diagnosis -> grep matches -> clean `PASS`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT * FROM deltaLakeAzure('\0', MD5('')) SETTINGS allow_experimental_delta_kernel_rs = 1" 2>&1 \
    | grep -o -m1 BAD_ARGUMENTS
