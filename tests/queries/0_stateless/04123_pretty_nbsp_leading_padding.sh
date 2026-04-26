#!/usr/bin/env bash
# Verify the output_format_pretty_use_nbsp_for_leading_padding setting renders
# the leading row-number padding (and the indent before grid borders) with
# the UTF-8 encoding of U+00A0 NO-BREAK SPACE (0xC2 0xA0) instead of an ASCII
# space (0x20).
# See https://github.com/ClickHouse/ClickHouse/issues/95122

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Default: every padded row begins with at least one ASCII space (0x20).
# We dump the first three bytes of the second line (the column header row,
# which carries leading row-number padding) and look for a leading 0x20.
default_first_bytes=$(
    $CLICKHOUSE_CLIENT --query="SELECT 1 AS x FORMAT PrettyCompact" \
        | sed -n '1p' | head -c 3 | od -An -tx1 | tr -d ' \n'
)
# UTF-8 encoding of U+00A0 (NO-BREAK SPACE) is 0xC2 0xA0.
nbsp_first_bytes=$(
    $CLICKHOUSE_CLIENT \
        --output_format_pretty_use_nbsp_for_leading_padding=1 \
        --query="SELECT 1 AS x FORMAT PrettyCompact" \
        | sed -n '1p' | head -c 3 | od -An -tx1 | tr -d ' \n'
)
ascii_charset_first_bytes=$(
    $CLICKHOUSE_CLIENT \
        --output_format_pretty_use_nbsp_for_leading_padding=1 \
        --output_format_pretty_grid_charset=ASCII \
        --query="SELECT 1 AS x FORMAT PrettyCompact" \
        | sed -n '1p' | head -c 3 | od -An -tx1 | tr -d ' \n'
)

echo "default first byte is ASCII space: $([[ ${default_first_bytes:0:2} == 20 ]] && echo yes || echo no)"
echo "nbsp first bytes are UTF-8 U+00A0: $([[ ${nbsp_first_bytes:0:4} == c2a0 ]] && echo yes || echo no)"
echo "ASCII charset first byte stays ASCII space: $([[ ${ascii_charset_first_bytes:0:2} == 20 ]] && echo yes || echo no)"
