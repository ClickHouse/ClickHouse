#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The variant object {"a.b": 1, "a": {"b": 2}} has the path `a.b` twice once nested objects are
# flattened into `JSON` paths.
DATA_FILE=$CUR_DIR/data_parquet/05298_variant_json_duplicate_path.parquet

${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}', Parquet)" 2>&1 | grep -o -m1 'INCORRECT_DATA'
