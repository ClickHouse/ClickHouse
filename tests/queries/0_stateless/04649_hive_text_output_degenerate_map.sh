#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `map()` is typed `Map(Nothing, Nothing)`. An empty map never invokes the element serializers, so
# without an upfront check on the key type it would be formatted successfully, even though Hive
# declares maps as `MAP<key_type, data_type>` and has no type to put in place of `Nothing`.
${CLICKHOUSE_CLIENT} --query "SELECT map() FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Map(.*) is not supported by the HiveText output format: Hive requires a concrete primitive type as a Map key"

# The same map nested inside supported wrappers must be rejected too.
${CLICKHOUSE_CLIENT} --query "SELECT [map()] FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Map(.*) is not supported by the HiveText output format: Hive requires a concrete primitive type as a Map key"
${CLICKHOUSE_CLIENT} --query "SELECT map('a', map()) FORMAT HiveText" 2>&1 \
    | grep -o -m1 "Type Map(.*) is not supported by the HiveText output format: Hive requires a concrete primitive type as a Map key"

# `Nothing` stays accepted everywhere it is not a Map key: a `Nothing` map value serializes the same
# way as any other type would, exactly like the top-level `NULL` and `[]` literals.
${CLICKHOUSE_CLIENT} --query "SELECT map('a', NULL) FORMAT HiveText" | tr '\002\003' ';:'
${CLICKHOUSE_CLIENT} --query "SELECT CAST(map(), 'Map(String, Nothing)') FORMAT HiveText" | tr '\002\003' ';:'
