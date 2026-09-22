#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=04510_select_access_rights_rewrite.lib
. "$CUR_DIR"/04510_select_access_rights_rewrite.lib

set -euo pipefail

trap cleanup_objects EXIT
cleanup_objects

test_single_column
test_single_column_table_grant
test_all_columns
test_all_columns_table_grant
