#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An unshredded variant column whose middle row is null. With `input_format_null_as_default` the
# leaves are read as non-nullable and the null arrives as an empty blob instead, which is not a
# well-formed variant value (both leaves start with a header byte). It has to read as a null
# Dynamic, the same as through the null map.
DATA_FILE=$CUR_DIR/data_parquet/04932_variant_null.parquet

for null_as_default in 0 1; do
    echo "-- input_format_null_as_default = ${null_as_default}"
    ${CLICKHOUSE_LOCAL} --query="
        SELECT dynamicType(v) AS type, v
        FROM file('${DATA_FILE}', Parquet)
        SETTINGS input_format_null_as_default = ${null_as_default}"
done
