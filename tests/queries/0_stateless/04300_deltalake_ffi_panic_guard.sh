#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: depends on delta-kernel-rs and AWS (not built/available in fast test)
# Tag no-msan: delta-kernel-rs is not built with MSan

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104509 (failure mode 2).
#
# A NUL byte is valid UTF-8, so it passes the option decoding, but it is an invalid HTTP header
# value. While signing the S3 request, `object_store` does `.unwrap()` on the resulting
# `InvalidHeaderValue` and panics on a `TokioBackgroundExecutor` worker; the kernel executor then
# panics on the calling thread inside `ffi::snapshot`. Because the FFI is `extern "C"` (nounwind),
# that panic used to reach `panic_cannot_unwind` and abort the server. The FFI now wraps `snapshot`
# and `builder_build` in `catch_unwind`, converting the panic into a `DELTA_KERNEL_ERROR` exception.
#
# Written as a `.sh` test through `clickhouse-local`: on the unfixed binary the process aborts, so
# stdout is empty and mismatches the reference (clean FAIL); on the fixed binary the guarded panic
# is reported and grep matches (PASS). Running in `clickhouse-local` keeps a crash on the unfixed
# binary from poisoning the live server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --query "SELECT * FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', '\0', 'x') SETTINGS allow_experimental_delta_kernel_rs = 1" 2>&1 \
    | grep -o -m1 'panicked across the FFI boundary'
