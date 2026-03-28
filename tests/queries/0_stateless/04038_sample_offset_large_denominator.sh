#!/usr/bin/env bash

# Regression test for AST round-trip of SAMPLE ratios.
# Verifies that formatting a SAMPLE clause and parsing it back produces identical SQL,
# and that large values saturate instead of wrapping to wrong results.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function check_roundtrip()
{
    local query="$1"
    local first second
    first=$($CLICKHOUSE_FORMAT --oneline --query "$query" 2>&1) || { echo "FAIL: clickhouse-format failed for: $query"; echo "Got: $first"; return; }
    second=$($CLICKHOUSE_FORMAT --oneline --query "$first" 2>&1) || { echo "FAIL: clickhouse-format failed on round-trip for: $query"; echo "Got: $second"; return; }

    if [ "$first" = "$second" ]; then
        echo "OK"
    else
        echo "FAIL: AST round-trip mismatch for: $query"
        echo "First:  $first"
        echo "Second: $second"
    fi
}

# Verify the formatted output does not contain a forbidden pattern.
function check_not_contains()
{
    local query="$1"
    local forbidden="$2"
    local result
    result=$($CLICKHOUSE_FORMAT --oneline --query "$query" 2>&1) || { echo "FAIL: clickhouse-format failed for: $query"; echo "Got: $result"; return; }

    if echo "$result" | grep -qF "$forbidden"; then
        echo "FAIL: output contains '$forbidden' for: $query"
        echo "Got: $result"
    else
        echo "OK"
    fi
}

# Verify the formatted output contains an expected pattern.
function check_contains()
{
    local query="$1"
    local expected="$2"
    local result
    result=$($CLICKHOUSE_FORMAT --oneline --query "$query" 2>&1) || { echo "FAIL: clickhouse-format failed for: $query"; echo "Got: $result"; return; }

    if echo "$result" | grep -qF "$expected"; then
        echo "OK"
    else
        echo "FAIL: output does not contain '$expected' for: $query"
        echo "Got: $result"
    fi
}

# Original fuzzer crash: denominator 10^23 overflows UInt64.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1 / 10 OFFSET 1.1920928955078125e-7"

# Scientific notation with large negative exponent (denominator 10^20).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1e-20"

# Decimal with 25 digits after point — denominator is 10^25.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 0.0000000000000000000000001"

# Both SAMPLE and OFFSET have large denominators.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1e-10 OFFSET 1e-12"

# Explicit rational with numerator and denominator exceeding UInt64 max.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 99999999999999999999 / 100000000000000000001"

# Boundary: 20-digit denominator (10^19 fits in UInt64, 10^20 does not).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1 / 100000000000000000000"

# Boundary: 19-digit denominator (10^18, fits in UInt64 — should always have worked).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1 / 1000000000000000000"

# Large numerator in scientific notation.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 9999999999999999.0e-30"

# Many significant digits in the decimal part.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 0.123456789012345678901234567890"

# Fractional SAMPLE with fractional OFFSET (both as decimals).
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 0.00000001 OFFSET 0.99999999"

# SAMPLE in a subquery inside a JOIN (mirrors the original fuzzer-found pattern).
check_roundtrip "SELECT * FROM numbers(1) JOIN (SELECT number FROM numbers(1) SAMPLE 1 / 10 OFFSET 1e-8) AS sub ON sub.number = number"

# Extreme exponent that would cause DoS with an unbounded loop. Must return instantly.
check_roundtrip "SELECT 1 FROM numbers(1) SAMPLE 1e-2000000000"

# Combined large fractional part + large negative exponent: denominator must not wrap to 1.
check_not_contains "SELECT 1 FROM numbers(1) SAMPLE 0.000000000000000000000000000000000000001e-39" "SAMPLE 1 / 1"

# Large integer.fraction where the multiply saturates but num_after > 0; must not wrap to small value.
check_not_contains "SELECT 1 FROM numbers(1) SAMPLE 99999999999999999999999999999999999999.999999999" "SAMPLE 0"

# Integer literal at 2^128 (39+ digits) must saturate to 2^128 - 1.
check_contains "SELECT 1 FROM numbers(1) SAMPLE 1 / 340282366920938463463374607431768211456" "/ 340282366920938463463374607431768211455"

# Valid 39-digit denominator below 2^128 must remain exact (no false saturation).
check_contains "SELECT 1 FROM numbers(1) SAMPLE 1 / 100000000000000000000000000000000000000" "/ 100000000000000000000000000000000000000"

# 20-digit denominator must not wrap to a small value.
check_not_contains "SELECT 1 FROM numbers(1) SAMPLE 1 / 100000000000000000000" "/ 0"

# 20-digit numerator in explicit rational must not wrap.
check_not_contains "SELECT 1 FROM numbers(1) SAMPLE 99999999999999999999 / 100000000000000000001" "SAMPLE 0"

# 39-digit value above 2^128-1 that wraps to a value >= 10^38 must still saturate.
check_contains "SELECT 1 FROM numbers(1) SAMPLE 1 / 500000000000000000000000000000000000000" "/ 340282366920938463463374607431768211455"

# Leading zeros must not inflate digit count and cause false saturation.
check_contains "SELECT 1 FROM numbers(1) SAMPLE 0000000000000000000000000000000000000001" "SAMPLE 1"
check_not_contains "SELECT 1 FROM numbers(1) SAMPLE 1 / 0000000000000000000000000000000000000001" "/ 340282366920938463463374607431768211455"
