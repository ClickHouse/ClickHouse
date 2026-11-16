#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Vortex has no address type, so `IPv4` goes out as `U32` and is inferred back as `UInt32`;
# naming the type explicitly gets `IPv4` back.

DATA_FILE=$CUR_DIR/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

echo "Schema inference:"
$CLICKHOUSE_LOCAL -q "
    SELECT
        toIPv4('1.2.3.4') AS ip,
        if(number % 2 = 0, NULL, toIPv4('255.255.255.255')) AS ip_nullable
    FROM numbers(2)
    FORMAT Vortex" > "$DATA_FILE"
$CLICKHOUSE_LOCAL -q "DESC file('$DATA_FILE', 'Vortex')"

echo "Round trip:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex') FORMAT TSV"

echo "Round trip with an explicit IPv4 schema:"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$DATA_FILE', 'Vortex', 'ip IPv4, ip_nullable Nullable(IPv4)') FORMAT TSV"

echo "IPv6 cannot be written:"
$CLICKHOUSE_LOCAL -q "SELECT toIPv6('2001:db8::1') AS ip FORMAT Vortex" > /dev/null 2>&1 && echo "unexpectedly succeeded" || echo "failed as expected"

rm -f "$DATA_FILE"
