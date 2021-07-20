#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="SELECT * FROM system.build_options" | perl -lnE 'print $1 if /(BUILD_DATE|BUILD_TYPE|CXX_COMPILER)\s+\S+/ || /(CXX_FLAGS|LINK_FLAGS|TZDATA_VERSION)/';
