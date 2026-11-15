#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Hive's LazySimpleSerDe reads FLOAT/DOUBLE with Java's parser, which spells non-finite values as
# `NaN`, `Infinity`, and `-Infinity`. ClickHouse's default `nan`/`inf`/`-inf` tokens would be read
# back by Hive as null, so HiveText output must emit the Java spellings for non-finite floats.
# Finite values keep their usual text. A visible fields delimiter keeps the reference readable.
${CLICKHOUSE_CLIENT} --query "SELECT
    toFloat64('nan') AS f64_nan,
    toFloat64('inf') AS f64_inf,
    toFloat64('-inf') AS f64_ninf,
    toFloat32('nan') AS f32_nan,
    toFloat32('inf') AS f32_inf,
    toFloat32('-inf') AS f32_ninf,
    toFloat64(1.5) AS f64_finite
    FORMAT HiveText
    SETTINGS input_format_hive_text_fields_delimiter = ','"

# The same must hold for non-finite floats nested inside collections.
${CLICKHOUSE_CLIENT} --query "SELECT CAST([toFloat64('nan'), toFloat64('inf'), toFloat64('-inf'), 2.5], 'Array(Float64)') AS a
    FORMAT HiveText
    SETTINGS input_format_hive_text_collection_items_delimiter = ';'"
