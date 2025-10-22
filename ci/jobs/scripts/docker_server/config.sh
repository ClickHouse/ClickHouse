#!/usr/bin/env bash

testAlias+=(
	[clickhouse / clickhouse - server]='clickhouse'
)

# Get current file directory
currentDir="${PWD}/ci/jobs/scripts/docker_server"

# interate over all directories in current path
for testDir in ${currentDir}/tests/*/; do
	customTestName = basename "${testDir}"
	imageTests["cliclhouse"]="${imageTests["clickhouse"]}	${customTestName}
	"
done
