#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Written by Spark 4.0.0: `SELECT cast(100000 as variant), cast('hello' as variant)`. An unshredded
# variant column is a group with a `value` and a `metadata` BYTE_ARRAY leaf, in that order, with no
# `VARIANT` logical type - Spark records the variant-ness only in its own key-value metadata
# `org.apache.spark.sql.parquet.row.metadata`. So the group has to be recognized by structure.
#
#   required group spark_schema {
#     required int32 n;
#     required group v {
#       required binary value;
#       required binary metadata;
#     }
#   }
DATA_FILE=$CUR_DIR/data_parquet/04928_variant_spark.parquet

${CLICKHOUSE_LOCAL} --stacktrace --query="SELECT n, v FROM file('${DATA_FILE}', Parquet) ORDER BY n" 
