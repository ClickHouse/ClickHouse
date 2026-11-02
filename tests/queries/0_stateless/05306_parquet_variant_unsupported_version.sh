#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A copy of 05296_variant_json_objects.parquet whose `VARIANT` logical type has
# `specification_version = 2`. Only version 1 exists, so the blobs may not have today's layout:
# the column is rejected instead of being decoded as version 1. The other columns are still readable.
DATA_FILE=$CUR_DIR/data_parquet/05306_variant_unsupported_version.parquet

${CLICKHOUSE_LOCAL} --query="SELECT n, v FROM file('${DATA_FILE}', Parquet) ORDER BY n" 2>&1 \
    | grep -o "Parquet column v is a variant with specification version 2, but only version 1 is supported"

${CLICKHOUSE_LOCAL} --query="SELECT count(), max(n) FROM file('${DATA_FILE}', Parquet, 'n Int32')"
