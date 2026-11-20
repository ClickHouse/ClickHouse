#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-cpu-aarch64, no-llvm-coverage
# Tag no-fasttest: avoid dependency on qemu -- inconvenient when running locally

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# If we run sanitized binary under qemu, it will try to slowly allocate 20 TiB until OOM.
# Don't even try to do that. This test should be disabled for sanitizer builds.
${CLICKHOUSE_LOCAL} --query "SELECT max(value LIKE '%sanitize%') FROM system.build_options" | grep -q '1' && echo '@@SKIP@@: Sanitizer build' && exit

if [ "$( ${CLICKHOUSE_LOCAL} -q "SELECT value FROM system.build_options where name = 'USE_OPENSSL_FIPS' LIMIT 1")" == "1" ]; then
    echo "@@SKIP@@: FIPS build"
    exit 0
fi

if ! hash qemu-x86_64-static 2>/dev/null; then
    echo "@@SKIP@@: No qemu-x86_64-static"
    exit 0
fi

command=$(command -v ${CLICKHOUSE_LOCAL})

function run_with_cpu()
{
    # Limit memory to 1 GB to fail fast if a sanitized binary is run under QEMU
    # Use --data instead of -m because RLIMIT_RSS does not work since Linux 2.6.x
    prlimit --data=5000000000 qemu-x86_64-static -cpu "$@" "$command" --query "SELECT 1" 2>&1 | \
      grep -v -F "warning: TCG doesn't support requested feature" | \
      grep -v -F 'Unknown host IFA type' ||:
}

run_with_cpu qemu64
run_with_cpu qemu64,+ssse3
run_with_cpu qemu64,+ssse3,+sse4.1
run_with_cpu qemu64,+ssse3,+sse4.1,+sse4.2
run_with_cpu qemu64,+ssse3,+sse4.1,+sse4.2,+popcnt
