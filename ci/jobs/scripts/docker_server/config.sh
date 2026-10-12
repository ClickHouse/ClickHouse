#!/usr/bin/env bash

# Get current file directory
currentDir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

# iterate over all directories in current path
clickhouseTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'clickhouse-*' -not -name 'clickhouse-distroless-*' -type d -exec basename {} \; )
keeperTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'keeper-*' -type d -exec basename {} \; )

# Distroless images have no shell or coreutils inside the container, so run
# only tests known to work with that constraint.
clickhouseDistrolessTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'clickhouse-distroless-*' -type d -exec basename {} \; )
clickhouseDistrolessSafeTests="clickhouse-basics ${clickhouseDistrolessTests}"
keeperDistrolessTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'keeper-distroless-*' -type d -exec basename {} \; )
keeperDistrolessSafeTests="clickhouse-distroless-no-shell ${keeperDistrolessTests}"

# Distroless images cannot run the official-images global tests that assume a
# shell/coreutils are available. Mark these image variants as explicit so the
# runner uses only the imageTests listed below instead of adding global tests.
# Arch-suffixed local CI tags are handled in docker_server.py.
explicitTests+=(
	['clickhouse/clickhouse-server:distroless']=1
	['clickhouse/clickhouse-keeper:distroless']=1
)

imageTests+=(
	['clickhouse/clickhouse-server']="${clickhouseTests}"
	['clickhouse/clickhouse-server:distroless']="${clickhouseDistrolessSafeTests}"
	['clickhouse/clickhouse-keeper']="${keeperTests}"
	['clickhouse/clickhouse-keeper:distroless']="${keeperDistrolessSafeTests}"
)
