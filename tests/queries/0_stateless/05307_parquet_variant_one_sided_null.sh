#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `metadata` and `value` together are one variant value, so in an unshredded variant they can only
# be null together, when the whole group is null (row 4). A null in just one of them (rows 2 and 3)
# is malformed and must be reported, not read as NULL.
#
#   optional group v {
#     optional binary metadata;
#     optional binary value;
#   }
DATA_FILE=$CUR_DIR/data_parquet/05307_variant_one_sided_null.parquet

${CLICKHOUSE_LOCAL} --query="SELECT n, v FROM file('${DATA_FILE}', Parquet) WHERE n IN (1, 4) ORDER BY n"

for n in 2 3; do
    ${CLICKHOUSE_LOCAL} --query="SELECT v FROM file('${DATA_FILE}', Parquet) WHERE n = $n" 2>&1 \
        | grep -o "Malformed Parquet variant column 'v': a row has [^:]*"
done
