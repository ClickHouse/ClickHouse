#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: the `encrypt` case depends on OpenSSL

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/104526
# The ordinal of the offending function argument must be spelled correctly in error messages:
# proper English suffixes (1st, 2nd, 3rd, 4th, ..., 11th, 12th, 13th, ..., 21st, 22nd, 23rd, ...),
# and for variadic functions the reported position must not be shifted by the number of mandatory arguments.

present() { grep -F -q "$2" <<< "$1" && echo "OK" || echo "FAIL"; }
absent() { grep -F -q "$2" <<< "$1" && echo "FAIL" || echo "OK"; }

# Original issue: the 4th argument of `encrypt` was rendered as "3th".
out=$(${CLICKHOUSE_LOCAL} --query="SELECT encrypt('aes-256-gcm', 'plaintext', '12345678901234567890123456789012', 12345)" 2>&1)
present "$out" "4th argument 'IV'"
absent "$out" "3th argument 'IV'"

# A variadic function must report the actual position (here the 2nd argument), not one shifted by the
# number of mandatory arguments.
out=$(${CLICKHOUSE_LOCAL} --query="SELECT printf('%d', [1, 2, 3])" 2>&1)
present "$out" "2nd argument 'sub'"
absent "$out" "3rd argument 'sub'"

# `tupleConcat` is variadic; build a call whose k-th argument is not a Tuple and check the ordinal.
tuple_concat_at() {
    local k=$1 args=() j
    for ((j = 1; j < k; j++)); do args+=("(1, 2)"); done
    args+=("42")
    local IFS=,
    echo "SELECT tupleConcat(${args[*]})"
}

for spec in "1:1st" "11:11th" "12:12th" "13:13th" "21:21st" "22:22nd" "23:23rd"; do
    k=${spec%%:*}
    ord=${spec#*:}
    out=$(${CLICKHOUSE_LOCAL} --query="$(tuple_concat_at "$k")" 2>&1)
    present "$out" "${ord} argument 'tupleN'"
done
