#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# If we run sanitized binary under qemu, it will try to slowly allocate 20 TiB until OOM.
# Don't even try to do that. This test should be disabled for sanitizer builds.
${CLICKHOUSE_LOCAL} --query "SELECT max(value LIKE '%sanitize%') FROM system.build_options" | grep -q '1' && echo 'Skip test for sanitizer build' && exit

command=$(command -v ${CLICKHOUSE_LOCAL})

function run_with_cpu()
{
    qemu-x86_64-static -cpu "$@" "$command" --query "SELECT 1" 2>&1 | grep -v -F "warning: TCG doesn't support requested feature" ||:
}

run_with_cpu qemu64
run_with_cpu qemu64,+ssse3
run_with_cpu qemu64,+ssse3,+sse4.1
run_with_cpu qemu64,+ssse3,+sse4.1,+sse4.2
run_with_cpu qemu64,+ssse3,+sse4.1,+sse4.2,+popcnt
