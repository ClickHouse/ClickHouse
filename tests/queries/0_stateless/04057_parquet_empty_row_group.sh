#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Parquet files with empty row groups (num_rows == 0) should be readable without errors.
${CLICKHOUSE_LOCAL} --query="SELECT * FROM file('$CUR_DIR/data_parquet/04057_empty_row_group.parquet') FORMAT TabSeparatedWithNamesAndTypes"
