#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression for the UUID2 binary wire format (RowBinary / Native).
# UUID2 sorts lexicographically, so it must be written as the canonical big-endian 16 bytes,
# unlike UUID which stores the two 64-bit halves swapped.

uuid="00112233-4455-6677-8899-aabbccddeeff"

# 1. On-wire byte order in RowBinary: exactly the 16 canonical big-endian bytes.
#    Read the raw output back as FixedString(16) to inspect the emitted bytes.
echo "RowBinary wire bytes (UUID2):"
$CLICKHOUSE_CLIENT --query "SELECT toUUID2('$uuid') FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format RowBinary --structure "x FixedString(16)" --query "SELECT hex(x) FROM table"

# For contrast, plain UUID writes the two halves swapped (not lexicographically ordered).
echo "RowBinary wire bytes (UUID, half-swapped for contrast):"
$CLICKHOUSE_CLIENT --query "SELECT toUUID('$uuid') FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format RowBinary --structure "x FixedString(16)" --query "SELECT hex(x) FROM table"

# 2. RowBinary round-trip: reading the bytes back as UUID2 returns the same textual value.
echo "RowBinary round-trip:"
$CLICKHOUSE_CLIENT --query "SELECT toUUID2('$uuid') FORMAT RowBinary" \
    | $CLICKHOUSE_LOCAL --input-format RowBinary --structure "x UUID2" --query "SELECT x FROM table"

# 3. Native round-trip: the type and value survive a full Native round-trip.
echo "Native round-trip:"
$CLICKHOUSE_CLIENT --query "SELECT toUUID2('$uuid') AS x FORMAT Native" \
    | $CLICKHOUSE_LOCAL --input-format Native --query "SELECT toTypeName(x), x FROM table"
