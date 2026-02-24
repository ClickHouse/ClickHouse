#!/usr/bin/env bash

# Get current file directory
currentDir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

# interate over all directories in current path
clickhouseTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'clickhouse-*' -type d -exec basename {} \; )
keeperTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'keeper-*' -type d -exec basename {} \; )

imageTests+=(
	['clickhouse/clickhouse-server']="${clickhouseTests}"
	['clickhouse/clickhouse-keeper']="${keeperTests}"
)
