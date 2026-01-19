#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$CUR_DIR/scripts_udf

# "result" is used as a virtual column name:
#
#   src/Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.cpp:    String result_name = "result";
#
$CLICKHOUSE_LOCAL --input_format_tsv_detect_header=1 -q "select udf_proxy(*) from values('result', 'foo')" -- --user_scripts_path="$SCRIPTS_DIR" --user_defined_executable_functions_config="$SCRIPTS_DIR/proxy.xml"
