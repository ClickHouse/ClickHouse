#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hive's LazySimpleSerDe has a fixed list of 8 separators indexed by nesting depth, so a type tree
# deep enough to need a ninth separator must be rejected upfront, from the declared header: an
# empty over-deep collection would otherwise serialize successfully (the deeper serializers are
# never reached), and the first non-empty value would fail only after formatting has started.
DEEP8='Array(Array(Array(Array(Array(Array(Array(Array(UInt8))))))))'
${CLICKHOUSE_CLIENT} --query "SELECT CAST([], '$DEEP8') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "The data is nested too deeply for the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST([[[[[[[[1]]]]]]]], '$DEEP8') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "The data is nested too deeply for the HiveText output format"

# A Map needs one more level than an Array or a Tuple at the same depth (separator N between
# entries and N + 1 between a key and its value), so a Map as the 7th nested collection is
# over-deep as well.
DEEP_MAP='Array(Array(Array(Array(Array(Array(Map(String, UInt8)))))))'
${CLICKHOUSE_CLIENT} --query "SELECT CAST([], '$DEEP_MAP') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "The data is nested too deeply for the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST([[[[[[map('k', 1)]]]]]], '$DEEP_MAP') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "The data is nested too deeply for the HiveText output format"

# Tuples count the same as arrays, and unlike an Array a Tuple value is never empty.
DEEP_TUPLE='Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(Tuple(UInt8, UInt8), UInt8), UInt8), UInt8), UInt8), UInt8), UInt8), UInt8)'
${CLICKHOUSE_CLIENT} --query "SELECT CAST((((((((( 1, 2), 3), 4), 5), 6), 7), 8), 9), '$DEEP_TUPLE') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "The data is nested too deeply for the HiveText output format"

# Positive controls: exactly 8 separator levels (the fields delimiter plus 7 nested ones) must
# keep working, both for empty and for non-empty values. Control-character separators are
# remapped to letters for readability: \x01 -> A, \x02 -> B, ..., \x08 -> H.
DEEP7='Array(Array(Array(Array(Array(Array(Array(UInt8)))))))'
${CLICKHOUSE_CLIENT} --query "SELECT CAST([], '$DEEP7') FORMAT HiveText" \
    | tr '\001\002\003\004\005\006\007\010' 'ABCDEFGH'
${CLICKHOUSE_CLIENT} --query "SELECT CAST([[[[[[[1, 2]]]]]]], '$DEEP7') FORMAT HiveText" \
    | tr '\001\002\003\004\005\006\007\010' 'ABCDEFGH'
${CLICKHOUSE_CLIENT} --query "SELECT [[[[[map('k', 1)]]]]] FORMAT HiveText" \
    | tr '\001\002\003\004\005\006\007\010' 'ABCDEFGH'
