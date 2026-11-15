#!/usr/bin/env bash
# Formatting of the fractional part of a Decimal emits whole blocks of digits at a time.
# Sweep every scale of every Decimal type; the scale of `toDecimal*` has to be a constant,
# so the queries are generated here.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for type_and_max_scale in "32 9" "64 18" "128 38" "256 76"; do
    read -r bits max_scale <<< "$type_and_max_scale"
    echo "Decimal$bits, every scale"

    # The whole part takes one digit, so the largest scale that fits '1.5' is one below the maximum.
    query=""
    for ((scale = 1; scale < max_scale; ++scale)); do
        query+="SELECT $scale, toString(toDecimal$bits('1.5', $scale)), toString(toDecimal$bits('-1.5', $scale));"
    done
    # At the maximal scale, every digit is fractional.
    query+="SELECT $max_scale, toString(toDecimal$bits('0.5', $max_scale)), toString(toDecimal$bits('-0.5', $max_scale));"
    $CLICKHOUSE_CLIENT --query "$query"
done

# `toDecimalString` rounds the fractional part to a fixed width and pads it with zeros;
# its precision has to be a constant as well.
echo "toDecimalString: precision below, equal to and above the scale"
query=""
for p in 0 1 2 3 4 7; do
    query+="SELECT $p, toDecimalString(toDecimal64('1.239', 3), $p);"
done
for p in 0 1 18 19 36 37 38 40; do
    query+="SELECT $p, toDecimalString(toDecimal128('1.0000000000000000000000000000000000009', 37), $p);"
done
for p in 0 1 2 20 60 76; do
    query+="SELECT $p, toDecimalString(toDecimal256('-0.5', 2), $p);"
done
$CLICKHOUSE_CLIENT --query "$query"

$CLICKHOUSE_CLIENT --query "
SELECT 'the maximal scale of every Decimal type';
SELECT toDecimal32('0.123456789', 9), toDecimal64('0.123456789012345678', 18);
SELECT toDecimal128('0.12345678901234567890123456789012345678', 38);
SELECT toDecimal256('0.1234567890123456789012345678901234567890123456789012345678901234567890123456', 76);

SELECT 'zero, trailing and leading zeros of the fractional part';
SELECT toDecimal64(0, 6), toDecimal64('0.000001', 6), toDecimal64('0.100000', 6), toDecimal64('-0.000001', 6), toDecimal64('0.010203', 6);
SELECT toDecimal128('0.00000000000000000000000000000000000001', 38), toDecimal128('0.10000000000000000000000000000000000000', 38);
SELECT toDecimal256('0.' || repeat('0', 75) || '1', 76), toDecimal256('-0.' || repeat('0', 75) || '1', 76);

SELECT 'whole part and fractional part of every width';
SELECT toDecimal64(x, 9) FROM (SELECT arrayJoin(['0.123456789', '1.02003004', '123456789.987654321', '-123456789.987654321']) AS x);
SELECT toDecimal128('123456789012345678901234567890.12345678', 8), toDecimal128('-123456789012345678901234567890.12345678', 8);
SELECT toDecimal256('12345678901234567890123456789012345678901234567890.123456789012345678901234', 24);

SELECT 'the boundary of a 64-bit fractional part';
SELECT toDecimal128('0.999999999999999999', 18), toDecimal128('0.9999999999999999999', 19), toDecimal128('0.99999999999999999999', 20);
SELECT toDecimal256('1.' || repeat('9', 30), 30);

SELECT 'the same values through other output formats';
SELECT toDecimal64('0.100000', 6) AS a, toDecimal128('-0.010203', 6) AS b, toDecimal256('0.' || repeat('0', 40) || '7', 41) AS c FORMAT CSV;
SELECT toDecimal64('0.100000', 6) AS a, toDecimal128('-0.010203', 6) AS b, toDecimal256('0.' || repeat('0', 40) || '7', 41) AS c FORMAT JSONEachRow;
SELECT toDecimal64('0.100000', 6) AS a, toDecimal128('-0.010203', 6) AS b, toDecimal256('0.' || repeat('0', 40) || '7', 41) AS c FORMAT TSV;

SELECT 'random values must round-trip through their text form, mismatches per type';
WITH
    arrayStringConcat(arrayMap(i -> leftPad(toString(reinterpretAsUInt64(randomString(8)) % 100000000000000000), 17, '0'), range(5))) AS digits,
    toDecimal64(substring(digits, 1, 9) || '.' || substring(digits, 10, 9), 9) AS d64,
    toDecimal128(substring(digits, 1, 19) || '.' || substring(digits, 20, 19), 19) AS d128,
    toDecimal256(substring(digits, 1, 38) || '.' || substring(digits, 39, 38), 38) AS d256
SELECT
    countIf(toDecimal64(toString(d64), 9) != d64),
    countIf(toDecimal128(toString(d128), 19) != d128),
    countIf(toDecimal256(toString(d256), 38) != d256)
FROM numbers(2000);
"
