#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=04516_role_rewrite.lib
. "$CUR_DIR"/04516_role_rewrite.lib

set -euo pipefail

trap cleanup EXIT
setup_role_test_table

test_create_role
test_grant_role_to_role
test_combine_privileges
test_admin_option
