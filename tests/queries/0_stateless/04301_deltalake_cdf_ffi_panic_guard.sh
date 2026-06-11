#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: depends on delta-kernel-rs and AWS (not built/available in fast test)
# Tag no-msan: delta-kernel-rs is not built with MSan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104509 on the Change Data
# Feed path. With `delta_lake_snapshot_start_version` set, the query goes through
# `ffi::table_changes_from_version` instead of `ffi::snapshot`. That entry point reads the log
# (object_store I/O) too, so the same invalid-HTTP-header credential (a NUL byte in the S3 access
# key) panics on a `TokioBackgroundExecutor` worker; the executor then panics on the calling thread
# inside `table_changes_from_version`, which is `extern "C"` (nounwind) and aborts the server. The
# CDF FFI entry points are now wrapped in `catch_unwind`, converting the panic into a
# `DELTA_KERNEL_ERROR` exception.
#
# Written as a `.sh` test through `clickhouse-local`: on the unfixed binary the process aborts, so
# stdout is empty and mismatches the reference (clean FAIL); on the fixed binary the guarded panic
# is reported and grep matches (PASS). Running in `clickhouse-local` keeps a crash on the unfixed
# binary from poisoning the live server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT * FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', '\0', 'x') SETTINGS allow_experimental_delta_kernel_rs = 1, delta_lake_snapshot_start_version = 0" 2>&1 \
    | grep -o -m1 'panicked across the FFI boundary'
