#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that \N is correctly parsed as NULL for Variant type in HTTP query parameters.
# Previously, readEscapedString would interpret \N as an empty string before the NULL
# check could see it, causing either an error (when no String variant) or a silent
# empty string instead of NULL (when String variant is present).

# \N for Variant without String: should be NULL, not an error
echo "Variant(UInt64, Int32) with NULL:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_var=%5CN" \
    -d "SELECT {var:Variant(UInt64, Int32)} AS val, val IS NULL FORMAT TabSeparated"

# \N for Variant with String: should be NULL, not empty string
echo "Variant(UInt64, String) with NULL:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_var=%5CN" \
    -d "SELECT {var:Variant(UInt64, String)} AS val, val IS NULL FORMAT TabSeparated"

# Regular values should still work
echo "Variant(UInt64, Int32) with value:"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_var=42" \
    -d "SELECT {var:Variant(UInt64, Int32)} AS val, variantType(val) FORMAT TabSeparated"

# Test via format() function with TSV input: Variant without String
echo "format() Variant(UInt64, Int32) with NULL:"
${CLICKHOUSE_CLIENT} -q "SELECT val, val IS NULL FROM format(TSV, 'val Variant(UInt64, Int32)', '\\\\N\n42\n-1') FORMAT TabSeparated"
