#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `v` is an unannotated Spark variant group, inferred as `Dynamic` or `Tuple` depending on the setting.
# Each `clickhouse-local` starts with an empty schema cache, so the first query fills it and the second
# must still get the type its own setting asks for.
# The copy is aged with `touch -d`: a cache entry is dropped when the file's mtime is >= its registration second.
DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
cp "$CUR_DIR"/data_parquet/04928_variant_spark.parquet "$DATA_FILE"
touch -d "2000-01-01 00:00:00" "$DATA_FILE"
for first in 1 0; do
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('$DATA_FILE', Parquet) SETTINGS input_format_parquet_detect_variant_by_structure = $first, print_pretty_type_names = 0;
        DESC file('$DATA_FILE', Parquet) SETTINGS input_format_parquet_detect_variant_by_structure = $((1 - first)), print_pretty_type_names = 0;" \
        | awk -F'\t' '$1 == "v" {print $2}'
done
rm -f "$DATA_FILE"
