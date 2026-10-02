#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hive declares maps as MAP<primitive_type, data_type>, so a map key cannot be a nested
# (ARRAY/MAP/STRUCT) type. ClickHouse permits composite Map keys, and their elements would
# serialize fine on their own, so the HiveText output format must reject them explicitly:
# otherwise it would emit files that no Hive schema can read back as a map.
for expr in \
    "map([1, 2], 3)" \
    "map((1, 'a'), 3)" \
    "map(map(1, 2), 3)" \
    "[map([1], 2)]" \
    "tuple(map([1], 2))" \
    "map(1, map([1], 2))"
do
    ${CLICKHOUSE_CLIENT} --query "SELECT ${expr} FORMAT HiveText" 2>&1 \
        | grep -o -m1 "Type Map(.*) is not supported by the HiveText output format: Hive supports only primitive types as Map keys"
done

# A composite-key Map hidden inside a supported wrapper must be rejected too: the validator has to
# unwrap Nullable before descending into Tuple, otherwise the check is bypassed and the file is written.
${CLICKHOUSE_CLIENT} --enable_nullable_tuple_type 1 \
    --query "SELECT CAST((map([1], 2),), 'Nullable(Tuple(Map(Array(UInt8), UInt8)))') FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Map(.*) is not supported by the HiveText output format: Hive supports only primitive types as Map keys"

# Maps with primitive keys are supported, including maps whose values are of nested types.
${CLICKHOUSE_CLIENT} --query "SELECT map('a', [1, 2], 'b', [3]) FORMAT HiveText" | tr '\002\003\004' ';:|'
