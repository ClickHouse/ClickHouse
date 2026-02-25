#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-cpu-aarch64, no-llvm-coverage
# Tag no-fasttest: avoid dependency on qemu -- inconvenient when running locally

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! hash qemu-x86_64-static 2>/dev/null; then
    echo "@@SKIP@@: No qemu-x86_64-static"
    exit 0
fi

if [ "$( ${CLICKHOUSE_LOCAL} -q "SELECT value FROM system.build_options where name = 'USE_OPENSSL_FIPS' LIMIT 1")" == "1" ]; then
    echo "@@SKIP@@: FIPS build"
    exit 0
fi

# x86-64-v3 guarantees AVX2, so simdjson always uses its Haswell implementation.
# There is no fallback path to test, and QEMU AVX2 emulation is too slow.
if ${CLICKHOUSE_LOCAL} --query "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'" 2>/dev/null | grep -qP 'x86-64-v[3-9]'; then
    echo "@@SKIP@@: x86-64-v3+ build (simdjson fallback not applicable)"
    exit 0
fi

command=$(command -v ${CLICKHOUSE_LOCAL})
# Limit memory to 5 GB to fail fast if a sanitized binary is run under QEMU
# (sanitized binaries try to allocate ~20 TiB of virtual memory for shadow memory)
# Use --data instead of -m because RLIMIT_RSS does not work since Linux 2.6.x
prlimit --data=5000000000 qemu-x86_64-static -cpu qemu64,+ssse3,+sse4.1,+sse4.2,+popcnt "$command" --allow_simdjson=1 "select JSONExtractRaw('{\"foo\": 1}', 'foo')"
