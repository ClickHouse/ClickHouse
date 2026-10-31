#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `data_parquet/04695_variant_nested_per_row_metadata.parquet` holds a shredded `VARIANT` column
# `json` of type `JSON(max_dynamic_paths=0, arr Array(JSON(max_dynamic_paths=0)))` with three rows,
# where each row carries a *different* `metadata` dictionary:
#
#   {"arr":[{"a":1},{"a":"s","p1":7}],"k0":"x"}
#   {"arr":[{"a":3,"p2":8}],"k2":"y"}
#   {"arr":[{"a":4},{"a":5},{"a":6,"p3":9}],"k9":"z"}
#
# The elements of `arr` are nested `VARIANT` wrappers with no `metadata` child of their own: they
# reuse the dictionary of the top-level row that owns them. Reading them therefore needs a mapping
# from element row back to top-level row, which the reader used to reject outright.
#
# ClickHouse's own writer always emits one shared dictionary per column chunk, so the file was made
# by writing it with ClickHouse (`output_format_parquet_json_as_variant = 1`,
# `output_format_parquet_compression_method = 'none'`,
# `output_format_parquet_max_dictionary_size = 0`) and then renaming the last residual key in the
# first and third row's `PLAIN`-encoded `metadata` blobs (`k1` -> `k0`, `k3` -> `k9`) and
# recomputing the data page CRC.

FILE="$CURDIR/data_parquet/04695_variant_nested_per_row_metadata.parquet"
TYPE='json JSON(max_dynamic_paths=0, arr Array(JSON(max_dynamic_paths=0)))'

$CLICKHOUSE_LOCAL --enable_json_type 1 --input_format_parquet_use_native_reader_v3 1 --query "
SELECT json FROM file('$FILE', Parquet, '$TYPE');
SELECT json FROM file('$FILE', Parquet, 'json String');
SELECT json.arr FROM file('$FILE', Parquet, '$TYPE');
"
