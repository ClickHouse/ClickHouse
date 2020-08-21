#!/usr/bin/env bash

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "CREATE TABLE system.columns AS numbers(10);" 2>&1 | grep -o "Code: 57"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE system.columns engine=Memory AS numbers(10);" 2>&1 | grep -o "Code: 62"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE system.columns AS numbers(10) Engine=Memory;" 2>&1 | grep -o "Code: 62"
