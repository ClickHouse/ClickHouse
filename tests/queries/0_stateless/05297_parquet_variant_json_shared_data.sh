#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A variant object with 1100 fields: paths over the `max_dynamic_paths` limit (1024) of `JSON`
# go to the shared data.
DATA_FILE=$CUR_DIR/data_parquet/05297_variant_json_shared_data.parquet

${CLICKHOUSE_LOCAL} --query "
    SELECT
        length(JSONDynamicPaths(j)),
        length(JSONSharedDataPaths(j)),
        j.k0000,
        j.k1023,
        j.k1099,
        dynamicType(j.k1099)
    FROM (SELECT dynamicElement(v, 'JSON') AS j FROM file('${DATA_FILE}', Parquet))
"
