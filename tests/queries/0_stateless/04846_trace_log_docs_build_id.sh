#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

python3 - "$CUR_DIR/../../../utils/generate-system-tables-docs" <<'PY'
import runpy
import sys

generator = runpy.run_path(sys.argv[1])
generate_columns_markdown = generator["generate_columns_markdown"]

aliases = [
    {
        "table": "trace_log",
        "name": "build_id",
        "default_expression": "'BINARY_SPECIFIC_BUILD_ID'",
    },
    {
        "table": "other_table",
        "name": "other_alias",
        "default_expression": "other_column",
    },
]

print(generate_columns_markdown([], aliases), end="")
PY
