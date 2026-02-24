#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that clickhouse help returns 0
$CLICKHOUSE_BINARY help >/dev/null 2>&1
echo "clickhouse help: $?"

# Test that clickhouse --help returns 0 (goes to local)
$CLICKHOUSE_BINARY --help >/dev/null 2>&1
echo "clickhouse --help: $?"

# Test that clickhouse start --help returns 0
$CLICKHOUSE_BINARY start --help >/dev/null 2>&1
echo "clickhouse start --help: $?"

# Test that clickhouse stop --help returns 0
$CLICKHOUSE_BINARY stop --help >/dev/null 2>&1
echo "clickhouse stop --help: $?"

# Test that clickhouse status --help returns 0
$CLICKHOUSE_BINARY status --help >/dev/null 2>&1
echo "clickhouse status --help: $?"

# Test that clickhouse restart --help returns 0
$CLICKHOUSE_BINARY restart --help >/dev/null 2>&1
echo "clickhouse restart --help: $?"

# Test that clickhouse install --help returns 0
$CLICKHOUSE_BINARY install --help >/dev/null 2>&1
echo "clickhouse install --help: $?"

# Test that clickhouse format --help returns 0
$CLICKHOUSE_BINARY format --help >/dev/null 2>&1
echo "clickhouse format --help: $?"

# Test that clickhouse extract-from-config --help returns 0
$CLICKHOUSE_BINARY extract-from-config --help >/dev/null 2>&1
echo "clickhouse extract-from-config --help: $?"

# Test that clickhouse git-import --help returns 0
$CLICKHOUSE_BINARY git-import --help >/dev/null 2>&1
echo "clickhouse git-import --help: $?"

# Test that clickhouse hash-binary --help returns 0
$CLICKHOUSE_BINARY hash-binary --help >/dev/null 2>&1
echo "clickhouse hash-binary --help: $?"

# Test that clickhouse benchmark --help returns 0
$CLICKHOUSE_BINARY benchmark --help >/dev/null 2>&1
echo "clickhouse benchmark --help: $?"

# Test that clickhouse compressor --help returns 0
$CLICKHOUSE_BINARY compressor --help >/dev/null 2>&1
echo "clickhouse compressor --help: $?"

# Test that clickhouse obfuscator --help returns 0
$CLICKHOUSE_BINARY obfuscator --help >/dev/null 2>&1
echo "clickhouse obfuscator --help: $?"

# Test that clickhouse server --help returns 0
$CLICKHOUSE_BINARY server --help >/dev/null 2>&1
echo "clickhouse server --help: $?"

# Test that clickhouse keeper --help returns 0
$CLICKHOUSE_BINARY keeper --help >/dev/null 2>&1
echo "clickhouse keeper --help: $?"

# Test that clickhouse client --help returns 0
$CLICKHOUSE_BINARY client --help >/dev/null 2>&1
echo "clickhouse client --help: $?"

# Test that clickhouse local --help returns 0
$CLICKHOUSE_BINARY local --help >/dev/null 2>&1
echo "clickhouse local --help: $?"
