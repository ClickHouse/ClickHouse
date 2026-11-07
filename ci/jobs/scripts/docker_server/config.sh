#!/usr/bin/env bash

# Get current file directory
currentDir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

# iterate over all directories in current path
clickhouseTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'clickhouse-*' -not -name 'clickhouse-distroless-*' -type d -exec basename {} \; )
keeperTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'keeper-*' -not -name 'keeper-distroless-*' -type d -exec basename {} \; )

# Distroless images have no shell or coreutils inside the container, so run
# only tests known to work with that constraint.
clickhouseDistrolessTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'clickhouse-distroless-*' -type d -exec basename {} \; )
clickhouseDistrolessSafeTests="clickhouse-basics ${clickhouseDistrolessTests}"
keeperDistrolessTests=$( find "$currentDir"/tests/ -maxdepth 1 -name 'keeper-distroless-*' -type d -exec basename {} \; )
keeperDistrolessSafeTests="clickhouse-distroless-no-shell ${keeperDistrolessTests}"

# The default server image is built `FROM scratch`: no shell or coreutils inside either, but, unlike the
# distroless one, it starts as root and prepares the directories like `entrypoint.sh`, so every server test
# runs on it. The tests must therefore not run programs other than `clickhouse` inside the container.
# The official-images `override-cmd` test supports single-binary images.
clickhouseScratchTests="override-cmd ${clickhouseTests} ${clickhouseDistrolessTests}"

# Distroless images cannot run the official-images global tests that assume a
# shell/coreutils are available. Mark these image variants as explicit so the
# runner uses only the imageTests listed below instead of adding global tests.
# Arch-suffixed local CI tags, including the un-suffixed `scratch` server image,
# are handled in docker_library.py.
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
