#!/usr/bin/env bash
# Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-fasttest, no-cpu-aarch64
# Tag no-fasttest: avoid dependency on qemu -- inconvenient when running locally

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! hash qemu-x86_64-static 2>/dev/null; then
    echo "@@SKIP@@: No qemu-x86_64-static"
    exit 0
fi

command=$(command -v ${CLICKHOUSE_LOCAL})
qemu-x86_64-static -cpu qemu64,+ssse3,+sse4.1,+sse4.2,+popcnt "$command" --allow_simdjson=1 "select JSONExtractRaw('{\"foo\": 1}', 'foo')"
