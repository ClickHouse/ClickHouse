#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A value-taking option must accept an empty value written adjacent to '=' (`--opt=`),
# the same as `--opt ""` and `set opt=''`. Regression test for issue #90987, where
# `clickhouse-local --format_csv_null_representation=''` failed with
# "the argument for option ... should follow immediately after the equal sign".
$CLICKHOUSE_LOCAL --format_csv_null_representation='' --query "SELECT 'a', NULL, 'b' FORMAT CSV"

# A zero-token switch must still reject an adjacent empty value (it must not be silently enabled).
$CLICKHOUSE_LOCAL --no-system-tables= --query "SELECT 1" |& grep -o "the argument for option '--no-system-tables' should follow immediately after the equal sign"
