#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$CUR_DIR/scripts_udf

$CLICKHOUSE_LOCAL -q 'select test_function()' --input_format_tsv_detect_header=1 -- --user_scripts_path=$SCRIPTS_DIR --user_defined_executable_functions_config=$SCRIPTS_DIR/function.xml
