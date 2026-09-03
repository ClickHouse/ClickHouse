#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./02530_dictionaries_update_field.lib
. "$CUR_DIR"/02530_dictionaries_update_field.lib

run_test_with_layout "flat"
