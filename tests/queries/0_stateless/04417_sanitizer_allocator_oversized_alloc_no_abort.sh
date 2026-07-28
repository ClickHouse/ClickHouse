#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With allocator_may_return_null=1 an oversized single allocation returns null (recoverable)
# instead of aborting, so the "server alive" query after it still runs. ASan/MSan also print a
# benign "<Sanitizer> failed to allocate <N> bytes" warning; TSan and non-sanitizer builds
# recover silently. Route the sanitizer log to our own file (last log_path wins, so it stays out
# of the runner's fatal log) and normalize the warning to its build-invariant substring with
# "grep -o" so one .reference matches every build.
log="${CLICKHOUSE_TMP:-.}/04417_$$"
rm -f "$log"*
o="log_path=$log"
ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    $CLICKHOUSE_LOCAL --query "SYSTEM ALLOCATE MEMORY 999999999999999999; SELECT 'server alive'"
grep -hom1 'failed to allocate' "$log"* 2>/dev/null || echo 'failed to allocate'
rm -f "$log"*
