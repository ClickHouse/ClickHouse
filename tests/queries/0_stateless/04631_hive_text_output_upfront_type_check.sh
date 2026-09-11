#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unsupported types must be rejected from the declared header, not per value: `SerializationNullable`
# writes `\N` without descending into the nested serializer, and empty `Array`/`Map` values never
# invoke the element serializer at all. Without an upfront check on the header types, the queries
# below would silently produce a file whose declared schema no Hive table could have.
${CLICKHOUSE_CLIENT} --query "SELECT CAST(NULL, 'Nullable(Time)') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Time is not supported by the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST(NULL, 'Nullable(Int128)') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Int128 is not supported by the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST([], 'Array(Decimal(39, 2))') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Decimal precision 39 is not supported by the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST([], 'Array(Enum8(''a'' = 1))') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Enum is not supported by the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST(map(), 'Map(String, Time64(3))') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Time64 is not supported by the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST(NULL, 'Nullable(UUID)') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type UUID is not supported by the HiveText output format"
${CLICKHOUSE_CLIENT} --query "SELECT CAST([], 'Array(IntervalSecond)') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Interval is not supported by the HiveText output format"

# The bare NULL and empty-array literals are typed `Nullable(Nothing)` and `Array(Nothing)`.
# `Nothing` has no values to serialize, so it must stay accepted by the upfront check.
${CLICKHOUSE_CLIENT} --query "SELECT NULL FORMAT HiveText"
${CLICKHOUSE_CLIENT} --query "SELECT [], CAST([], 'Array(UInt8)'), CAST(NULL, 'Nullable(String)') FORMAT HiveText" | tr '\001' ';'
