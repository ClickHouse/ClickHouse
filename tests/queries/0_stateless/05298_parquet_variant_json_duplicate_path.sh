#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The variant object {"a.b": 1, "a": {"b": 2}} would have the path `a.b` twice once nested objects
# are flattened into `JSON` paths. The dot inside the key is escaped, so both values are kept.
DATA_FILE=$CUR_DIR/data_parquet/05298_variant_json_duplicate_path.parquet

${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}', Parquet)"
${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}', Parquet, 'v JSON')"
