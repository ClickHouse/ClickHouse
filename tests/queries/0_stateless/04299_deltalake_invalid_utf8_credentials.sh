#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: depends on delta-kernel-rs (not built in fast test)
# Tag no-msan: delta-kernel-rs is not built with MSan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104509
#
# A binary credential (e.g. raw `MD5(...)` bytes used as a SAS token) used to abort the
# server: the `delta-kernel-rs` FFI `set_builder_option` called `.unwrap()` on invalid UTF-8,
# and the resulting panic crossed the `extern "C"` boundary and triggered `panic_in_cleanup`.
# The C++ side now validates the option value and rejects invalid UTF-8 with `BAD_ARGUMENTS`
# (and `set_builder_option` also returns an error instead of panicking as defense in depth).
#
# Written as a `.sh` test (not `.sql` with `{ serverError }`) so Bugfix validation can record a
# clean test-level result against the unfixed binary: the unfixed code aborts the process, so
# stdout is empty (no error text) and mismatches the reference -> clean FAIL; the fixed binary
# prints the diagnosis -> grep matches -> clean PASS. Using `clickhouse-local` keeps a crash on
# the unfixed binary from poisoning the live server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT * FROM deltaLakeAzure('\0', UNHEX('D41D8CD98F00B204E9800998ECF8427E')) SETTINGS allow_experimental_delta_kernel_rs = 1" 2>&1 \
    | grep -o -m1 BAD_ARGUMENTS
