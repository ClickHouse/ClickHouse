#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-cpu-aarch64
# Tag no-fasttest: avoid dependency on qemu -- invonvenient when running locally

# More than a decade after AVX was released, AVX is still not supported by QEMU, even if "-cpu help" pretends to. As a result, we cannot use
# QEMU to verify that a ClickHouse binary compiled for a SIMD level up to AVX runs on a system with a SIMD level up to AVX. The alternative
# is to disassemble the binary and grep for unwanted instructions (e.g. AVX512) which is just too fragile ...
#
# https://gitlab.com/qemu-project/qemu/-/issues/164
# https://www.mail-archive.com/qemu-devel@nongnu.org/msg713932.html
# https://lore.kernel.org/all/CAObpvQmejWBh+RNz2vhk16-kcY_QveM_pSmM5ZeWqWv1d8AJzQ@mail.gmail.com/T/

exit 0

# keeping the original test because it is instructive and maybe QEMU will be fixed at some point ...

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# If we run sanitized binary under qemu, it will try to slowly allocate 20 TiB until OOM.
# Don't even try to do that. This test should be disabled for sanitizer builds.
${CLICKHOUSE_LOCAL} --query "SELECT max(value LIKE '%sanitize%') FROM system.build_options" | grep -q '1' && echo '@@SKIP@@: Sanitizer build' && exit

command=$(command -v ${CLICKHOUSE_LOCAL})

if ! hash qemu-x86_64-static 2>/dev/null; then
    echo "@@SKIP@@: No qemu-x86_64-static"
    exit 0
fi

function run_with_cpu()
{
    qemu-x86_64-static -cpu "$@" "$command" --query "SELECT 1" 2>&1 | grep -v -F "warning: TCG doesn't support requested feature" ||:
}

run_with_cpu qemu64
run_with_cpu qemu64,+ssse3
run_with_cpu qemu64,+ssse3,+sse4.1
run_with_cpu qemu64,+ssse3,+sse4.1,+sse4.2
run_with_cpu qemu64,+ssse3,+sse4.1,+sse4.2,+popcnt,+avx,+avx2
