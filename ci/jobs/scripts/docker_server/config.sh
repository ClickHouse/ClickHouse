#!/usr/bin/env bash

testAlias+=(
	['clickhouse/clickhouse-server']='clickhouse'
)

# Get current file directory
currentDir="${PWD}/ci/jobs/scripts/docker_server"

# interate over all directories in current path
imageTestsDefinition='
	'
for testDir in "${currentDir}"/tests/*/; do
  customTestName=$(basename "${testDir}")
	imageTestsDefinition="${imageTestsDefinition}	${customTestName}
	"
done

imageTests+=(
	['clickhouse']="${imageTestsDefinition}"
)
