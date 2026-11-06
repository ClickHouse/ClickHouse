#!/usr/bin/env bash
# Tags: no-fasttest, long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05216_orc_nested_null_as_default_pushdown.lib
. "$CUR_DIR"/05216_orc_nested_null_as_default_pushdown.lib

set -euo pipefail

write_orc_file
run_cases 1
