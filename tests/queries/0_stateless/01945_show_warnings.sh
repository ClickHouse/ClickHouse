#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

BUILD_TYPE=$("${CLICKHOUSE_CLIENT}" --multiquery --query "SELECT value FROM system.build_options WHERE name='BUILD_TYPE'")

if [ $BUILD_TYPE == "Debug" ]
then ./01945_show_debug_warning.expect
fi