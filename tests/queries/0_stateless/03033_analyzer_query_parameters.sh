#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

clickhouse-local --param_rounding 1 --query "SELECT 1 AS x ORDER BY x WITH FILL STEP {rounding:UInt32} SETTINGS allow_experimental_analyzer = 1"
clickhouse-local --param_rounding 1 --query "SELECT 1 AS x ORDER BY x WITH FILL STEP {rounding:UInt32} SETTINGS allow_experimental_analyzer = 0"
